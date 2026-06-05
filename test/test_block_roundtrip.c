/*
 * test_block_roundtrip.c -- encode a block via chc_block_builder, feed it
 * to `clickhouse local --input-format Native -q 'SELECT ...'`, & verify
 * the values come back through clickhouse-local's own decoder.
 */

#include <assert.h>
#include <inttypes.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#include "clickhouse-posix-io.h"

static int fail_count = 0;
static const char *current_test = "";

#include "test_common.h"

/* Spawn `clickhouse local --input-format Native -q "<sql>"`, returning a
 * pipe to feed bytes in. The child's stdout is captured into `child_out`
 * if non-NULL, otherwise inherited from this process. */
static pid_t
spawn_local_input(const char *sql, const char *struct_clause,
                  int *write_fd, int *read_fd)
{
    int in_pipe[2], out_pipe[2];
    if (pipe(in_pipe) < 0) return -1;
    if (pipe(out_pipe) < 0) { close(in_pipe[0]); close(in_pipe[1]); return -1; }

    pid_t pid = fork();
    if (pid < 0) { close(in_pipe[0]); close(in_pipe[1]); close(out_pipe[0]); close(out_pipe[1]); return -1; }
    if (pid == 0) {
        dup2(in_pipe[0], STDIN_FILENO);
        dup2(out_pipe[1], STDOUT_FILENO);
        close(in_pipe[0]); close(in_pipe[1]);
        close(out_pipe[0]); close(out_pipe[1]);
        execlp("clickhouse", "clickhouse", "local",
               "--input-format", "Native",
               "--structure", struct_clause,
               "--output_format_native_encode_types_in_binary_format=0",
               "-q", sql, (char *) NULL);
        _exit(127);
    }
    close(in_pipe[0]);
    close(out_pipe[1]);
    *write_fd = in_pipe[1];
    *read_fd  = out_pipe[0];
    return pid;
}

static char *
read_all(int fd, size_t *out_len)
{
    size_t cap = 256, len = 0;
    char *buf = malloc(cap);
    for (;;) {
        if (len == cap) { cap *= 2; buf = realloc(buf, cap); }
        ssize_t n = read(fd, buf + len, cap - len);
        if (n <= 0) break;
        len += (size_t) n;
    }
    buf[len < cap ? len : cap - 1] = '\0';
    *out_len = len;
    return buf;
}

static void
test_write_uint32(void)
{
    current_test = "write_uint32";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    chc_block_builder *bb = NULL;
    if (chc_block_builder_init(&bb, &al, &err) < 0) {
        fprintf(stderr, "%s: init: %s\n", current_test, err.msg); fail_count++; return;
    }

    uint32_t a[5] = { 10, 20, 30, 40, 50 };
    chc_type *t = NULL;
    if (chc_type_parse("UInt32", 6, &al, &t, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }
    if (chc_block_builder_append_fixed(bb, "x", 1, t, a, 5, &err) < 0) {
        fprintf(stderr, "%s: append: %s\n", current_test, err.msg); fail_count++; return;
    }

    int wfd, rfd;
    pid_t pid = spawn_local_input("SELECT sum(x) FROM table",
                                  "x UInt32", &wfd, &rfd);
    CHECK(pid > 0);

    chc_posix_io state; chc_io io;
    chc_posix_io_init(&state, &io, wfd, NULL, NULL);
    if (chc_block_write(&io, bb, NULL, &err) < 0) {
        fprintf(stderr, "%s: write: %s\n", current_test, err.msg);
        fail_count++;
    }
    close(wfd);

    size_t out_len;
    char *out = read_all(rfd, &out_len);
    close(rfd);
    int status; waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);

    /* Default format is TSV with newline. Expect "150\n". */
    if (out_len < 3 || strncmp(out, "150", 3) != 0) {
        fprintf(stderr, "%s: unexpected output (len=%zu): %.*s\n",
                current_test, out_len, (int) out_len, out);
        fail_count++;
    }

    free(out);
    chc_type_destroy(t, &al);
    chc_block_builder_destroy(bb);
}

static void
test_write_string(void)
{
    current_test = "write_string";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    chc_block_builder *bb = NULL;
    if (chc_block_builder_init(&bb, &al, &err) < 0) {
        fprintf(stderr, "%s: init: %s\n", current_test, err.msg); fail_count++; return;
    }
    /* "ab", "cde", "" -> offsets {2, 5, 5}, bytes "abcde". */
    uint64_t offs[3] = { 2, 5, 5 };
    const uint8_t bytes[] = { 'a','b','c','d','e' };
    if (chc_block_builder_append_string(bb, "s", 1, offs, bytes, 3, &err) < 0) {
        fprintf(stderr, "%s: append: %s\n", current_test, err.msg); fail_count++; return;
    }

    int wfd, rfd;
    pid_t pid = spawn_local_input(
        "SELECT groupArray(s) FROM table FORMAT TSV",
        "s String", &wfd, &rfd);
    CHECK(pid > 0);

    chc_posix_io state; chc_io io;
    chc_posix_io_init(&state, &io, wfd, NULL, NULL);
    if (chc_block_write(&io, bb, NULL, &err) < 0) {
        fprintf(stderr, "%s: write: %s\n", current_test, err.msg);
        fail_count++;
    }
    close(wfd);

    size_t out_len;
    char *out = read_all(rfd, &out_len);
    close(rfd);
    int status; waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);

    /* TSV: ['ab','cde',''] */
    if (out_len < 1 || !strstr(out, "ab") || !strstr(out, "cde")) {
        fprintf(stderr, "%s: unexpected: %.*s\n", current_test,
                (int) out_len, out);
        fail_count++;
    }

    free(out);
    chc_block_builder_destroy(bb);
}

/* Drive `clickhouse local` with the built block, return captured TSV
 * output (caller frees) or NULL on spawn failure. */
static char *
capture_roundtrip(const char *test_name, chc_block_builder *bb,
                  const char *structure, const char *sql, size_t *out_len)
{
    chc_err err = {};
    int wfd, rfd;
    pid_t pid = spawn_local_input(sql, structure, &wfd, &rfd);
    CHECK(pid > 0);
    if (pid <= 0) return NULL;

    chc_posix_io state; chc_io io;
    chc_posix_io_init(&state, &io, wfd, NULL, NULL);
    if (chc_block_write(&io, bb, NULL, &err) < 0) {
        fprintf(stderr, "%s: write: %s\n", test_name, err.msg);
        fail_count++;
    }
    close(wfd);

    char *out = read_all(rfd, out_len);
    close(rfd);
    int status; waitpid(pid, &status, 0);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
    return out;
}

/* Drive `clickhouse local` with a built block & assert each substring
 * in `expect_substrings` (NULL-terminated) appears in TSV output. */
static void
run_roundtrip(const char *test_name,
              chc_block_builder *bb,
              const char *structure,
              const char *sql,
              const char **expect_substrings)
{
    size_t out_len;
    char *out = capture_roundtrip(test_name, bb, structure, sql, &out_len);
    if (!out) return;
    for (const char **p = expect_substrings; *p; p++) {
        if (!strstr(out, *p)) {
            fprintf(stderr, "%s: missing substring '%s' in output: %.*s\n",
                    test_name, *p, (int) out_len, out);
            fail_count++;
        }
    }
    free(out);
}

static void
test_write_nullable_fixed(void)
{
    current_test = "write_nullable_fixed";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    chc_block_builder *bb = NULL;
    if (chc_block_builder_init(&bb, &al, &err) < 0) {
        fprintf(stderr, "%s: init: %s\n", current_test, err.msg); fail_count++; return;
    }
    chc_type *t = NULL;
    if (chc_type_parse("Nullable(UInt32)", 16, &al, &t, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }
    /* Rows: NULL, 100, NULL, 200. Inner cells under NULL are arbitrary. */
    uint8_t  nulls[4]  = { 1, 0, 1, 0 };
    uint32_t values[4] = { 0, 100, 0, 200 };
    if (chc_block_builder_append_nullable_fixed(bb, "x", 1, t,
            nulls, values, 4, &err) < 0) {
        fprintf(stderr, "%s: append: %s\n", current_test, err.msg); fail_count++;
    }

    const char *expect[] = { "300\t2", NULL };
    run_roundtrip(current_test, bb,
                  "x Nullable(UInt32)",
                  "SELECT sum(x), countIf(x IS NULL) FROM table",
                  expect);

    chc_type_destroy(t, &al);
    chc_block_builder_destroy(bb);
}

static void
test_write_nullable_string(void)
{
    current_test = "write_nullable_string";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    chc_block_builder *bb = NULL;
    if (chc_block_builder_init(&bb, &al, &err) < 0) {
        fprintf(stderr, "%s: init: %s\n", current_test, err.msg); fail_count++; return;
    }
    chc_type *t = NULL;
    if (chc_type_parse("Nullable(String)", 16, &al, &t, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }
    /* Rows: "a", NULL, "bc". Inner offsets/data still cover all 3 strings. */
    uint8_t  nulls[3]   = { 0, 1, 0 };
    uint64_t offs[3]    = { 1, 1, 3 };
    const uint8_t buf[] = "abc";
    if (chc_block_builder_append_nullable_string(bb, "s", 1, t,
            nulls, offs, buf, 3, &err) < 0) {
        fprintf(stderr, "%s: append: %s\n", current_test, err.msg); fail_count++;
    }

    const char *expect[] = { "1\t2", NULL };
    run_roundtrip(current_test, bb,
                  "s Nullable(String)",
                  "SELECT countIf(s IS NULL), countIf(s IS NOT NULL) FROM table",
                  expect);

    chc_type_destroy(t, &al);
    chc_block_builder_destroy(bb);
}

static void
test_write_array_fixed(void)
{
    current_test = "write_array_fixed";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    chc_block_builder *bb = NULL;
    if (chc_block_builder_init(&bb, &al, &err) < 0) {
        fprintf(stderr, "%s: init: %s\n", current_test, err.msg); fail_count++; return;
    }
    chc_type *t = NULL;
    if (chc_type_parse("Array(UInt32)", 13, &al, &t, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }
    /* Rows: [1,2,3], [4,5]. Cumulative ends: [3, 5]. */
    uint64_t offs[2]    = { 3, 5 };
    uint32_t values[5]  = { 1, 2, 3, 4, 5 };
    if (chc_block_builder_append_array_fixed(bb, "a", 1, t,
            offs, values, 2, &err) < 0) {
        fprintf(stderr, "%s: append: %s\n", current_test, err.msg); fail_count++;
    }

    const char *expect[] = { "15\t5", NULL };
    run_roundtrip(current_test, bb,
                  "a Array(UInt32)",
                  "SELECT sum(arraySum(a)), sum(length(a)) FROM table",
                  expect);

    chc_type_destroy(t, &al);
    chc_block_builder_destroy(bb);
}

static void
test_write_array_string(void)
{
    current_test = "write_array_string";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    chc_block_builder *bb = NULL;
    if (chc_block_builder_init(&bb, &al, &err) < 0) {
        fprintf(stderr, "%s: init: %s\n", current_test, err.msg); fail_count++; return;
    }
    chc_type *t = NULL;
    if (chc_type_parse("Array(String)", 13, &al, &t, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }
    /* Rows: ['x', 'yy'], ['zzz']. */
    uint64_t arr_offs[2]    = { 2, 3 };
    uint64_t val_offs[3]    = { 1, 3, 6 };
    const uint8_t buf[]     = "xyyzzz";
    if (chc_block_builder_append_array_string(bb, "a", 1, t,
            arr_offs, val_offs, buf, 2, &err) < 0) {
        fprintf(stderr, "%s: append: %s\n", current_test, err.msg); fail_count++;
    }

    const char *expect[] = { "xyyzzz", NULL };
    run_roundtrip(current_test, bb,
                  "a Array(String)",
                  "SELECT arrayStringConcat(arrayFlatten(groupArray(a))) FROM table",
                  expect);

    chc_type_destroy(t, &al);
    chc_block_builder_destroy(bb);
}

static void
test_write_array_nested_fixed(void)
{
    current_test = "write_array_nested_fixed";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    chc_block_builder *bb = NULL;
    if (chc_block_builder_init(&bb, &al, &err) < 0) {
        fprintf(stderr, "%s: init: %s\n", current_test, err.msg); fail_count++; return;
    }
    chc_type *t = NULL;
    if (chc_type_parse("Array(Array(UInt32))", 20, &al, &t, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }
    /* Rows: [[1,2],[3]], [[4,5,6]].
     * Outer level: row 0 has 2 inner arrays, row 1 has 1 -> {2,3}.
     * Inner level: inner arrays of sizes 2,1,3 -> cumulative {2,3,6}. */
    uint64_t l0[2] = { 2, 3 };
    uint64_t l1[3] = { 2, 3, 6 };
    const uint64_t *levels[2] = { l0, l1 };
    size_t levels_len[2] = { 2, 3 };
    uint32_t values[6] = { 1, 2, 3, 4, 5, 6 };
    if (chc_block_builder_append_array_nested_fixed(bb, "a", 1, t,
            2, levels, levels_len, values, 2, &err) < 0) {
        fprintf(stderr, "%s: append: %s\n", current_test, err.msg); fail_count++;
    }

    /* arrayFlatten twice on groupArray(Array(Array(UInt32))) flattens to
     * Array(UInt32); sum = 21, total leaf count = 6. */
    const char *expect[] = { "21\t6", NULL };
    run_roundtrip(current_test, bb,
                  "a Array(Array(UInt32))",
                  "SELECT arraySum(arrayFlatten(arrayFlatten(groupArray(a)))), "
                  "length(arrayFlatten(arrayFlatten(groupArray(a)))) FROM table",
                  expect);

    chc_type_destroy(t, &al);
    chc_block_builder_destroy(bb);
}

static void
test_write_array_nested_fixed_3d(void)
{
    current_test = "write_array_nested_fixed_3d";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    chc_block_builder *bb = NULL;
    if (chc_block_builder_init(&bb, &al, &err) < 0) {
        fprintf(stderr, "%s: init: %s\n", current_test, err.msg); fail_count++; return;
    }
    chc_type *t = NULL;
    if (chc_type_parse("Array(Array(Array(UInt32)))", 27, &al, &t, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }
    /* Rows: [[[1],[2,3]],[[4]]], [[[5,6]]].
     * L0 (outer): row 0 has 2, row 1 has 1 -> {2,3}.
     * L1 (mid):   sizes 2,1,1 -> {2,3,4}.
     * L2 (inner): sizes 1,2,1,2 -> {1,3,4,6}. */
    uint64_t l0[2] = { 2, 3 };
    uint64_t l1[3] = { 2, 3, 4 };
    uint64_t l2[4] = { 1, 3, 4, 6 };
    const uint64_t *levels[3] = { l0, l1, l2 };
    size_t levels_len[3] = { 2, 3, 4 };
    uint32_t values[6] = { 1, 2, 3, 4, 5, 6 };
    if (chc_block_builder_append_array_nested_fixed(bb, "a", 1, t,
            3, levels, levels_len, values, 2, &err) < 0) {
        fprintf(stderr, "%s: append: %s\n", current_test, err.msg); fail_count++;
    }

    const char *expect[] = { "21\t6", NULL };
    run_roundtrip(current_test, bb,
                  "a Array(Array(Array(UInt32)))",
                  "SELECT arraySum(arrayFlatten(arrayFlatten(arrayFlatten(groupArray(a))))), "
                  "length(arrayFlatten(arrayFlatten(arrayFlatten(groupArray(a))))) FROM table",
                  expect);

    chc_type_destroy(t, &al);
    chc_block_builder_destroy(bb);
}

static void
test_write_array_nested_string(void)
{
    current_test = "write_array_nested_string";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    chc_block_builder *bb = NULL;
    if (chc_block_builder_init(&bb, &al, &err) < 0) {
        fprintf(stderr, "%s: init: %s\n", current_test, err.msg); fail_count++; return;
    }
    chc_type *t = NULL;
    if (chc_type_parse("Array(Array(String))", 20, &al, &t, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }
    /* Rows: [['x','yy'],['zzz']], [['a']].
     * L0: {2, 3}. L1 inner-arr sizes 2,1,1 -> {2,3,4}.
     * Leaf strings: 'x','yy','zzz','a' -> offs {1,3,6,7}, buf "xyyzzza". */
    uint64_t l0[2] = { 2, 3 };
    uint64_t l1[3] = { 2, 3, 4 };
    const uint64_t *levels[2] = { l0, l1 };
    size_t levels_len[2] = { 2, 3 };
    uint64_t val_offs[4] = { 1, 3, 6, 7 };
    const uint8_t buf[] = "xyyzzza";
    if (chc_block_builder_append_array_nested_string(bb, "a", 1, t,
            2, levels, levels_len, val_offs, buf, 2, &err) < 0) {
        fprintf(stderr, "%s: append: %s\n", current_test, err.msg); fail_count++;
    }

    /* Bad-ndim path: ndim=2 against a 1-level Array(String) type must
     * reject with CHC_ERR_TYPE. */
    chc_type *bad = NULL;
    chc_err  berr = {};
    if (chc_type_parse("Array(String)", 13, &al, &bad, &berr) == CHC_OK) {
        chc_err xerr = {};
        int rc = chc_block_builder_append_array_nested_string(bb, "x", 1, bad,
                2, levels, levels_len, val_offs, buf, 2, &xerr);
        CHECK(rc == CHC_ERR_TYPE);
        chc_type_destroy(bad, &al);
    }

    const char *expect[] = { "xyyzzza", NULL };
    run_roundtrip(current_test, bb,
                  "a Array(Array(String))",
                  "SELECT arrayStringConcat(arrayFlatten(arrayFlatten(groupArray(a)))) FROM table",
                  expect);

    chc_type_destroy(t, &al);
    chc_block_builder_destroy(bb);
}

static void
test_write_lc_string(void)
{
    current_test = "write_lc_string";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    chc_block_builder *bb = NULL;
    if (chc_block_builder_init(&bb, &al, &err) < 0) {
        fprintf(stderr, "%s: init: %s\n", current_test, err.msg); fail_count++; return;
    }
    chc_type *t = NULL;
    if (chc_type_parse("LowCardinality(String)", 22, &al, &t, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }
    /* Dict ["red","green","blue"]. Keys [0,1,2,0] -> red, green, blue, red. */
    uint8_t  keys[4]      = { 0, 1, 2, 0 };
    uint64_t dict_offs[3] = { 3, 8, 12 };
    const uint8_t dict_buf[] = "redgreenblue";
    if (chc_block_builder_append_low_cardinality_string(bb, "s", 1, t,
            1, keys, dict_offs, dict_buf, 3, 4, &err) < 0) {
        fprintf(stderr, "%s: append: %s\n", current_test, err.msg); fail_count++;
    }

    /* 4 total rows, 3 distinct, "red" appears twice. */
    const char *expect[] = { "4\t3\t2", NULL };
    run_roundtrip(current_test, bb,
                  "s LowCardinality(String)",
                  "SELECT count(), uniqExact(s), countIf(s='red') FROM table",
                  expect);

    chc_type_destroy(t, &al);
    chc_block_builder_destroy(bb);
}

static void
test_write_lc_nullable_string(void)
{
    current_test = "write_lc_nullable_string";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    chc_block_builder *bb = NULL;
    if (chc_block_builder_init(&bb, &al, &err) < 0) {
        fprintf(stderr, "%s: init: %s\n", current_test, err.msg); fail_count++; return;
    }
    chc_type *t = NULL;
    if (chc_type_parse("LowCardinality(Nullable(String))", 32, &al, &t, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }
    /* Dict ["", "a", "b"] (slot 0 = null sentinel). Keys [1, 0, 2, 0] ->
     * 'a', NULL, 'b', NULL. */
    uint8_t  keys[4]      = { 1, 0, 2, 0 };
    uint64_t dict_offs[3] = { 0, 1, 2 };
    const uint8_t dict_buf[] = "ab";
    if (chc_block_builder_append_low_cardinality_string(bb, "s", 1, t,
            1, keys, dict_offs, dict_buf, 3, 4, &err) < 0) {
        fprintf(stderr, "%s: append: %s\n", current_test, err.msg); fail_count++;
    }

    const char *expect[] = { "2\t2", NULL };
    run_roundtrip(current_test, bb,
                  "s LowCardinality(Nullable(String))",
                  "SELECT countIf(s IS NULL), countIf(s IS NOT NULL) FROM table",
                  expect);

    chc_type_destroy(t, &al);
    chc_block_builder_destroy(bb);
}

/*
 * INSERT-side of the JSON pair: builder emits an 8-byte LE version=1
 * prefix once before a String body. Server-side reads back the prefix
 * at SerializationObject.cpp:448 to pick STRING mode — no INSERT
 * setting needed. Output side normalises {"a":1} → {"a":"1"}; we just
 * confirm both documents round-trip in the toString projection.
 */
static void
test_write_json_string(void)
{
    current_test = "write_json_string";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};

    chc_block_builder *bb = NULL;
    if (chc_block_builder_init(&bb, &al, &err) < 0) {
        fprintf(stderr, "%s: init: %s\n", current_test, err.msg); fail_count++; return;
    }
    chc_type *t = NULL;
    if (chc_type_parse("JSON", 4, &al, &t, &err) < 0) {
        fprintf(stderr, "%s: type: %s\n", current_test, err.msg); fail_count++; return;
    }
    /* Rows: '{}', '{"a":1}' -> offsets {2, 9}, bytes '{}{"a":1}'. */
    uint64_t offs[2]    = { 2, 9 };
    const uint8_t buf[] = "{}{\"a\":1}";
    if (chc_block_builder_append_json_string(bb, "j", 1, t,
            offs, buf, 2, &err) < 0) {
        fprintf(stderr, "%s: append: %s\n", current_test, err.msg); fail_count++;
    }

    /* Mismatched-kind validation: same args against a String type must
     * error with CHC_ERR_TYPE, mirroring the other builder helpers. */
    chc_type *bad = NULL;
    chc_err  berr = {};
    if (chc_type_parse("String", 6, &al, &bad, &berr) == CHC_OK) {
        chc_err xerr = {};
        int rc = chc_block_builder_append_json_string(bb, "x", 1, bad,
                offs, buf, 2, &xerr);
        CHECK(rc == CHC_ERR_TYPE);
        chc_type_destroy(bad, &al);
    }

    /* CH 25.8+ widens through JSON parser to {"a":"1"}; older LTS keeps
     * {"a":1}. Accept either form. */
    size_t out_len;
    char *out = capture_roundtrip(current_test, bb, "j JSON",
                                  "SELECT toString(j) FROM table FORMAT TSV",
                                  &out_len);
    if (out) {
        CHECK(strstr(out, "{}") != NULL);
        CHECK(strstr(out, "{\"a\":\"1\"}") != NULL ||
              strstr(out, "{\"a\":1}") != NULL);
        free(out);
    }

    chc_type_destroy(t, &al);
    chc_block_builder_destroy(bb);
}

int main(void)
{
    test_write_uint32();
    test_write_string();
    test_write_nullable_fixed();
    test_write_nullable_string();
    test_write_array_fixed();
    test_write_array_string();
    test_write_array_nested_fixed();
    test_write_array_nested_fixed_3d();
    test_write_array_nested_string();
    test_write_lc_string();
    test_write_lc_nullable_string();
    test_write_json_string();
    if (fail_count) {
        fprintf(stderr, "FAIL: %d check(s)\n", fail_count);
        return 1;
    }
    fprintf(stderr, "ok\n");
    return 0;
}
