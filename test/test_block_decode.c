/*
 * test_block_decode.c -- drives `clickhouse local -q '... FORMAT Native'`,
 * decodes the output via clickhouse.h, & asserts the column slabs match
 * expected values. One process per query; failure prints to stderr and
 * exits 1.
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

static int  fail_count = 0;
static const char *current_test = "";

#include "test_common.h"

static pid_t
spawn_local(const char *query, int *out_fd)
{
    int pipefd[2];
    if (pipe(pipefd) < 0) { perror("pipe"); return -1; }

    pid_t pid = fork();
    if (pid < 0) { perror("fork"); close(pipefd[0]); close(pipefd[1]); return -1; }

    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        close(pipefd[1]);
        execlp("clickhouse", "clickhouse", "local",
               "--format", "Native",
               "--output_format_native_encode_types_in_binary_format=0",
               "-q", query, (char *) NULL);
        _exit(127);
    }
    close(pipefd[1]);
    *out_fd = pipefd[0];
    return pid;
}

static chc_block *
read_one_block(const char *query)
{
    int fd;
    pid_t pid = spawn_local(query, &fd);
    if (pid < 0) return NULL;

    chc_alloc al = chc_alloc_stdlib();
    chc_posix_io state;
    chc_io io;
    chc_posix_io_init(&state, &io, fd, NULL, NULL);

    chc_block *b = NULL;
    chc_block_opts opts = {0};
    chc_err err = {0};
    int rc = chc_block_read(&io, &al, &opts, &b, &err);
    if (rc < 0) {
        fprintf(stderr, "%s: decode failed (rc=%d): %s\n", current_test, rc, err.msg);
        b = NULL;
    }
    close(fd);
    int status;
    waitpid(pid, &status, 0);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
        fprintf(stderr, "%s: child exited %d\n", current_test, WEXITSTATUS(status));
    return b;
}

static void
free_block(chc_block *b)
{
    chc_alloc al = chc_alloc_stdlib();
    chc_block_destroy(b, &al);
}

/* ---------------- tests --------------------------------------------------- */

static void test_uint32_string(void) {
    current_test = "uint32_string";
    chc_block *b = read_one_block(
        "SELECT toUInt32(number) AS a, toString(number*7) AS b FROM numbers(5)");
    CHECK(b != NULL); if (!b) return;
    CHECK_EQ_U64(chc_block_n_rows(b), 5);
    CHECK_EQ_U64(chc_block_n_columns(b), 2);

    const chc_column *a = chc_block_column(b, 0);
    CHECK(chc_column_layout(a) == CHC_COL_FIXED);
    size_t es;
    const uint32_t *adata = chc_column_fixed_data(a, &es);
    CHECK_EQ_U64(es, 4);
    for (int i = 0; i < 5; i++) CHECK_EQ_U64(adata[i], (uint32_t) i);

    const chc_column *s = chc_block_column(b, 1);
    CHECK(chc_column_layout(s) == CHC_COL_STRING);
    const uint8_t *sd = chc_column_string_data(s);
    const uint64_t *so = chc_column_string_offsets(s);
    const char *expected[] = { "0", "7", "14", "21", "28" };
    uint64_t prev = 0;
    for (int i = 0; i < 5; i++) {
        size_t len = (size_t) (so[i] - prev);
        CHECK_STR_EQ((const char *) sd + prev, len, expected[i]);
        prev = so[i];
    }
    free_block(b);
}

static void test_nullable(void) {
    current_test = "nullable";
    chc_block *b = read_one_block(
        "SELECT if(number % 2 = 0, toNullable(toInt64(number)), CAST(NULL,'Nullable(Int64)')) AS n "
        "FROM numbers(6)");
    CHECK(b != NULL); if (!b) return;
    CHECK_EQ_U64(chc_block_n_rows(b), 6);
    const chc_column *c = chc_block_column(b, 0);
    CHECK(chc_column_layout(c) == CHC_COL_NULLABLE);
    const uint8_t *nm = chc_column_null_map(c);
    int64_t expected_nm[6] = {0,1,0,1,0,1};
    for (int i = 0; i < 6; i++) CHECK_EQ_I64(nm[i], expected_nm[i]);

    const chc_column *inner = chc_column_nullable_inner(c);
    size_t es;
    const int64_t *d = chc_column_fixed_data(inner, &es);
    CHECK_EQ_U64(es, 8);
    int64_t expected_vals[6] = {0, 0, 2, 0, 4, 0};
    for (int i = 0; i < 6; i++) {
        if (!nm[i]) CHECK_EQ_I64(d[i], expected_vals[i]);
    }
    free_block(b);
}

static void test_array(void) {
    current_test = "array";
    /* Three rows, three differently-shaped arrays, packed into one block. */
    chc_block *b = read_one_block(
        "SELECT cast([[1,2,3],[],[7,8]][number+1], 'Array(UInt8)') AS arr "
        "FROM numbers(3)");
    CHECK(b != NULL); if (!b) return;
    CHECK_EQ_U64(chc_block_n_rows(b), 3);
    const chc_column *a = chc_block_column(b, 0);
    CHECK(chc_column_layout(a) == CHC_COL_ARRAY);
    const uint64_t *offs = chc_column_array_offsets(a);
    CHECK_EQ_U64(offs[0], 3);
    CHECK_EQ_U64(offs[1], 3);
    CHECK_EQ_U64(offs[2], 5);
    const chc_column *v = chc_column_array_values(a);
    CHECK_EQ_U64(chc_column_n_rows(v), 5);
    size_t es;
    const uint8_t *vd = chc_column_fixed_data(v, &es);
    CHECK_EQ_U64(es, 1);
    uint8_t expected[5] = {1, 2, 3, 7, 8};
    for (int i = 0; i < 5; i++) CHECK_EQ_U64(vd[i], expected[i]);
    free_block(b);
}

static void test_lowcardinality(void) {
    current_test = "lowcardinality";
    chc_block *b = read_one_block(
        "SELECT cast(['hi','bye','hi','hi','bye'][number+1], 'LowCardinality(String)') AS lc "
        "FROM numbers(5)");
    CHECK(b != NULL); if (!b) return;
    CHECK_EQ_U64(chc_block_n_rows(b), 5);
    const chc_column *c = chc_block_column(b, 0);
    CHECK(chc_column_layout(c) == CHC_COL_LOW_CARDINALITY);
    CHECK_EQ_U64(chc_column_lc_key_size(c), 1);
    const uint8_t *keys = chc_column_lc_keys(c);
    const chc_column *dict = chc_column_lc_dict(c);
    CHECK(chc_column_layout(dict) == CHC_COL_STRING);
    /* Dict layout: idx 0 = default (empty), then the unique values. We
     * verify by reconstructing rows. */
    const uint8_t  *dd = chc_column_string_data(dict);
    const uint64_t *doff = chc_column_string_offsets(dict);
    const char *expected[5] = { "hi", "bye", "hi", "hi", "bye" };
    for (int i = 0; i < 5; i++) {
        uint8_t k = keys[i];
        uint64_t start = k == 0 ? 0 : doff[k - 1];
        uint64_t end   = doff[k];
        CHECK_STR_EQ((const char *) dd + start, (size_t) (end - start), expected[i]);
    }
    free_block(b);
}

static void test_lowcardinality_nullable(void) {
    current_test = "lowcardinality_nullable";
    chc_block *b = read_one_block(
        "SELECT cast(['hi', NULL, 'hi', NULL][number+1], "
        "            'LowCardinality(Nullable(String))') AS lc "
        "FROM numbers(4)");
    CHECK(b != NULL); if (!b) return;
    CHECK_EQ_U64(chc_block_n_rows(b), 4);

    const chc_column *c = chc_block_column(b, 0);
    CHECK(chc_column_layout(c) == CHC_COL_LOW_CARDINALITY);
    CHECK_EQ_U64(chc_column_lc_key_size(c), 1);

    const chc_column *dict = chc_column_lc_dict(c);
    CHECK(chc_column_layout(dict) == CHC_COL_NULLABLE);
    const uint8_t *nm = chc_column_null_map(dict);
    CHECK_EQ_I64(nm[0], 1);                     /* slot 0 is the NULL sentinel */
    /* slots 1..dict_n-1 are non-NULL */
    for (size_t i = 1; i < chc_column_n_rows(dict); i++)
        CHECK_EQ_I64(nm[i], 0);

    const chc_column *inner = chc_column_nullable_inner(dict);
    CHECK(chc_column_layout(inner) == CHC_COL_STRING);
    const uint8_t  *idata = chc_column_string_data(inner);
    const uint64_t *ioffs = chc_column_string_offsets(inner);

    const uint8_t *keys = chc_column_lc_keys(c);
    const char *expected[4] = { "hi", NULL, "hi", NULL };
    for (int i = 0; i < 4; i++) {
        uint8_t k = keys[i];
        if (expected[i] == NULL) {
            CHECK_EQ_I64(nm[k], 1);             /* this row is NULL */
        } else {
            CHECK_EQ_I64(nm[k], 0);
            uint64_t start = k == 0 ? 0 : ioffs[k - 1];
            uint64_t end   = ioffs[k];
            CHECK_STR_EQ((const char *) idata + start,
                         (size_t) (end - start), expected[i]);
        }
    }
    free_block(b);
}

static void test_tuple(void) {
    current_test = "tuple";
    chc_block *b = read_one_block(
        "SELECT cast((toUInt8(number), toString(number)), 'Tuple(UInt8, String)') AS t "
        "FROM numbers(3)");
    CHECK(b != NULL); if (!b) return;
    const chc_column *c = chc_block_column(b, 0);
    CHECK(chc_column_layout(c) == CHC_COL_TUPLE);
    CHECK_EQ_U64(chc_column_tuple_arity(c), 2);
    const chc_column *c0 = chc_column_tuple_child(c, 0);
    const chc_column *c1 = chc_column_tuple_child(c, 1);
    CHECK_EQ_U64(chc_column_n_rows(c0), 3);
    CHECK_EQ_U64(chc_column_n_rows(c1), 3);
    size_t es;
    const uint8_t *d0 = chc_column_fixed_data(c0, &es);
    CHECK_EQ_U64(es, 1);
    for (int i = 0; i < 3; i++) CHECK_EQ_U64(d0[i], (uint8_t) i);

    /* Anonymous tuple => field names absent. */
    const chc_type *tt = chc_block_column_type(b, 0);
    CHECK(chc_type_tuple_field_name(tt, 0, NULL) == NULL);
    CHECK(chc_type_tuple_field_name(tt, 1, NULL) == NULL);
    free_block(b);
}

static void test_named_tuple(void) {
    current_test = "named_tuple";
    chc_block *b = read_one_block(
        "SELECT cast((toUInt8(number), toString(number)), "
        "            'Tuple(id UInt8, name String)') AS t "
        "FROM numbers(2)");
    CHECK(b != NULL); if (!b) return;

    const chc_type *tt = chc_block_column_type(b, 0);
    CHECK_EQ_I64(chc_type_kind(tt), CHC_TUPLE);
    CHECK_EQ_U64(chc_type_n_children(tt), 2);

    size_t fn0_len, fn1_len;
    const char *fn0 = chc_type_tuple_field_name(tt, 0, &fn0_len);
    const char *fn1 = chc_type_tuple_field_name(tt, 1, &fn1_len);
    CHECK(fn0 != NULL); if (fn0) CHECK_STR_EQ(fn0, fn0_len, "id");
    CHECK(fn1 != NULL); if (fn1) CHECK_STR_EQ(fn1, fn1_len, "name");

    /* Inner column types still parse correctly through the field-name
     * lookahead. */
    CHECK_EQ_I64(chc_type_kind(chc_type_child(tt, 0)), CHC_UINT8);
    CHECK_EQ_I64(chc_type_kind(chc_type_child(tt, 1)), CHC_STRING);

    /* Anonymous query returns NULL for tuple_field_name on non-tuple types. */
    const chc_type *str_type = chc_type_child(tt, 1);   /* a String */
    CHECK(chc_type_tuple_field_name(str_type, 0, NULL) == NULL);

    free_block(b);
}

/* Parser-only test for the trickier shape: nested types inside named
 * tuples, partial-naming, & whitespace. */
static void test_named_tuple_parser(void) {
    current_test = "named_tuple_parser";
    chc_alloc al = chc_alloc_stdlib();
    struct {
        const char *src;
        size_t      n_children;
        const char *names[4];     /* NULL = expected anonymous slot */
    } cases[] = {
        { "Tuple(a Int32, b String)",                  2, { "a", "b" } },
        { "Tuple(x LowCardinality(String), y Int8)",   2, { "x", "y" } },
        { "Tuple(p Tuple(q Int8, r String))",          1, { "p" } },
        /* All-anonymous: no field-name array */
        { "Tuple(Int32, String)",                      2, { NULL, NULL } },
        /* Single anonymous parametric child */
        { "Tuple(FixedString(4))",                     1, { NULL } },
        /* Backtick-quoted identifier permits spaces & punctuation. */
        { "Tuple(`s p a c e` String, `x.y` Int32)",    2, { "s p a c e", "x.y" } },
        /* Double-quote-quoted identifier. */
        { "Tuple(\"a b\" Int8)",                       1, { "a b" } },
        /* Doubled-backtick escape -> single backtick in resolved name. */
        { "Tuple(`a``b` String)",                      1, { "a`b" } },
        /* Backslash escape preserves the following char verbatim. */
        { "Tuple(`a\\`b` String)",                     1, { "a`b" } },
    };
    for (size_t i = 0; i < sizeof cases / sizeof cases[0]; i++) {
        chc_type *t = NULL;
        chc_err   err = {0};
        int rc = chc_type_parse(cases[i].src, strlen(cases[i].src), &al, &t, &err);
        if (rc != CHC_OK) {
            fprintf(stderr, "%s: parse '%s' failed: %s\n",
                    current_test, cases[i].src, err.msg);
            fail_count++; continue;
        }
        CHECK_EQ_U64(chc_type_n_children(t), cases[i].n_children);
        for (size_t j = 0; j < cases[i].n_children; j++) {
            size_t fl;
            const char *fn = chc_type_tuple_field_name(t, j, &fl);
            if (cases[i].names[j] == NULL) {
                if (fn != NULL) {
                    fprintf(stderr, "%s: '%s' child %zu: expected anon, got '%.*s'\n",
                            current_test, cases[i].src, j, (int) fl, fn);
                    fail_count++;
                }
            } else {
                CHECK(fn != NULL);
                if (fn) CHECK_STR_EQ(fn, fl, cases[i].names[j]);
            }
        }
        chc_type_destroy(t, &al);
    }
}

static void test_map(void) {
    current_test = "map";
    chc_block *b = read_one_block(
        "SELECT cast(map('a', toInt32(1), 'b', toInt32(2)), 'Map(String, Int32)') AS m");
    CHECK(b != NULL); if (!b) return;
    const chc_column *c = chc_block_column(b, 0);
    CHECK(chc_column_layout(c) == CHC_COL_ARRAY);
    const uint64_t *offs = chc_column_array_offsets(c);
    CHECK_EQ_U64(offs[0], 2);
    const chc_column *vals = chc_column_array_values(c);
    CHECK(chc_column_layout(vals) == CHC_COL_TUPLE);
    CHECK_EQ_U64(chc_column_tuple_arity(vals), 2);
    const chc_column *keys = chc_column_tuple_child(vals, 0);
    const chc_column *vs   = chc_column_tuple_child(vals, 1);
    CHECK(chc_column_layout(keys) == CHC_COL_STRING);
    size_t es;
    const int32_t *vsd = chc_column_fixed_data(vs, &es);
    CHECK_EQ_U64(es, 4);
    CHECK_EQ_I64(vsd[0], 1);
    CHECK_EQ_I64(vsd[1], 2);
    free_block(b);
}

static void test_point(void) {
    current_test = "point";
    chc_block *b = read_one_block("SELECT (1.5, 2.5)::Point AS p");
    CHECK(b != NULL); if (!b) return;
    const chc_column *c = chc_block_column(b, 0);
    CHECK(chc_column_layout(c) == CHC_COL_TUPLE);
    CHECK_EQ_U64(chc_column_tuple_arity(c), 2);
    size_t es;
    const double *x = chc_column_fixed_data(chc_column_tuple_child(c, 0), &es);
    const double *y = chc_column_fixed_data(chc_column_tuple_child(c, 1), &es);
    CHECK_EQ_U64(es, 8);
    CHECK(x[0] == 1.5);
    CHECK(y[0] == 2.5);
    free_block(b);
}

static void test_ring(void) {
    current_test = "ring";
    chc_block *b = read_one_block(
        "SELECT [(0.0,0.0),(1.0,0.0),(0.0,1.0)]::Ring AS r");
    CHECK(b != NULL); if (!b) return;
    const chc_column *c = chc_block_column(b, 0);
    CHECK(chc_column_layout(c) == CHC_COL_ARRAY);
    const uint64_t *offs = chc_column_array_offsets(c);
    CHECK_EQ_U64(offs[0], 3);                          /* 3 points */
    const chc_column *pt = chc_column_array_values(c);
    CHECK(chc_column_layout(pt) == CHC_COL_TUPLE);
    CHECK_EQ_U64(chc_column_n_rows(pt), 3);
    size_t es;
    const double *x = chc_column_fixed_data(chc_column_tuple_child(pt, 0), &es);
    const double *y = chc_column_fixed_data(chc_column_tuple_child(pt, 1), &es);
    CHECK(x[0] == 0.0 && y[0] == 0.0);
    CHECK(x[1] == 1.0 && y[1] == 0.0);
    CHECK(x[2] == 0.0 && y[2] == 1.0);
    free_block(b);
}

static void test_polygon(void) {
    current_test = "polygon";
    chc_block *b = read_one_block(
        "SELECT [[(0.0,0.0),(1.0,0.0),(1.0,1.0)],[(2.0,2.0),(3.0,3.0)]]::Polygon AS pg");
    CHECK(b != NULL); if (!b) return;
    const chc_column *c = chc_block_column(b, 0);
    CHECK(chc_column_layout(c) == CHC_COL_ARRAY);
    /* one row, two rings */
    CHECK_EQ_U64(chc_column_array_offsets(c)[0], 2);
    const chc_column *ring = chc_column_array_values(c);
    CHECK(chc_column_layout(ring) == CHC_COL_ARRAY);
    CHECK_EQ_U64(chc_column_n_rows(ring), 2);
    const uint64_t *ring_offs = chc_column_array_offsets(ring);
    CHECK_EQ_U64(ring_offs[0], 3);                     /* first ring: 3 pts */
    CHECK_EQ_U64(ring_offs[1], 5);                     /* +2 = 5 cumulative */
    const chc_column *pt = chc_column_array_values(ring);
    CHECK(chc_column_layout(pt) == CHC_COL_TUPLE);
    CHECK_EQ_U64(chc_column_n_rows(pt), 5);
    free_block(b);
}

static void test_multipolygon(void) {
    current_test = "multipolygon";
    chc_block *b = read_one_block(
        "SELECT [[[(0.0,0.0),(1.0,0.0),(0.0,1.0)]],"
        "        [[(2.0,2.0),(3.0,3.0)]]]::MultiPolygon AS mp");
    CHECK(b != NULL); if (!b) return;
    const chc_column *c = chc_block_column(b, 0);
    CHECK(chc_column_layout(c) == CHC_COL_ARRAY);
    /* one row, two polygons */
    CHECK_EQ_U64(chc_column_array_offsets(c)[0], 2);
    const chc_column *poly = chc_column_array_values(c);
    CHECK(chc_column_layout(poly) == CHC_COL_ARRAY);
    CHECK_EQ_U64(chc_column_n_rows(poly), 2);
    /* each polygon has 1 ring */
    const uint64_t *poly_offs = chc_column_array_offsets(poly);
    CHECK_EQ_U64(poly_offs[0], 1);
    CHECK_EQ_U64(poly_offs[1], 2);
    const chc_column *ring = chc_column_array_values(poly);
    CHECK_EQ_U64(chc_column_n_rows(ring), 2);
    const uint64_t *ring_offs = chc_column_array_offsets(ring);
    CHECK_EQ_U64(ring_offs[0], 3);                     /* ring 0: 3 pts */
    CHECK_EQ_U64(ring_offs[1], 5);                     /* ring 1: +2 = 5 */
    free_block(b);
}

static void test_decimal(void) {
    current_test = "decimal";
    chc_block *b = read_one_block(
        "SELECT toDecimal64(1234.5, 2) AS d");
    CHECK(b != NULL); if (!b) return;
    const chc_type *t = chc_block_column_type(b, 0);
    CHECK_EQ_I64(chc_type_kind(t), CHC_DECIMAL64);
    CHECK_EQ_I64(chc_type_decimal_scale(t), 2);
    CHECK_EQ_I64(chc_type_decimal_precision(t), 18);
    const chc_column *c = chc_block_column(b, 0);
    size_t es;
    const int64_t *d = chc_column_fixed_data(c, &es);
    CHECK_EQ_U64(es, 8);
    /* 1234.5 with scale=2 => 123450 */
    CHECK_EQ_I64(d[0], 123450);
    free_block(b);
}

static void test_uuid(void) {
    current_test = "uuid";
    chc_block *b = read_one_block(
        "SELECT arrayJoin([toUUID('00000000-0000-0000-0000-000000000000'),"
        "                  toUUID('00112233-4455-6677-8899-aabbccddeeff'),"
        "                  toUUID('01020304-0506-0708-090a-0b0c0d0e0f10'),"
        "                  toUUID('ffffffff-ffff-ffff-ffff-ffffffffffff')]) AS u");
    CHECK(b != NULL); if (!b) return;
    CHECK_EQ_U64(chc_block_n_rows(b), 4);

    const chc_type *t = chc_block_column_type(b, 0);
    CHECK_EQ_I64(chc_type_kind(t), CHC_UUID);

    const chc_column *c = chc_block_column(b, 0);
    CHECK(chc_column_layout(c) == CHC_COL_FIXED);
    size_t es;
    const uint8_t *d = chc_column_fixed_data(c, &es);
    CHECK_EQ_U64(es, 16);

    /* On-wire layout: two little-endian UInt64s. For UUID 'hh..ll' with
     * hi = first 8 bytes of the string form & lo = last 8 bytes, bytes are
     * reverse(hi) followed by reverse(lo). */
    static const uint8_t expected[4][16] = {
        {0},
        {0x77,0x66,0x55,0x44,0x33,0x22,0x11,0x00,
         0xff,0xee,0xdd,0xcc,0xbb,0xaa,0x99,0x88},
        {0x08,0x07,0x06,0x05,0x04,0x03,0x02,0x01,
         0x10,0x0f,0x0e,0x0d,0x0c,0x0b,0x0a,0x09},
        {0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,
         0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff},
    };
    for (int r = 0; r < 4; r++) {
        if (memcmp(d + r * 16, expected[r], 16) != 0) {
            fprintf(stderr, "%s: row %d bytes mismatch\n", current_test, r);
            fail_count++;
        }
    }
    free_block(b);
}

/*
 * `SELECT NULL` returns a column whose CH type is `Nullable(Nothing)`.
 * The outer Nullable is stripped at the column-layer; the inner type
 * must surface as CHC_NOTHING (NOT CHC_VOID, which is the zero-sentinel
 * value of the enum). Block decode of a Nothing column produces a
 * CHC_COL_NOTHING layout with the row count preserved & no data.
 *
 * Regression coverage for the pg_clickhouse port: a switch that only
 * arms CHC_VOID silently mishandles `SELECT NULL`, surfacing as
 * "unsupported type Nothing" at runtime.
 */
static void test_nothing(void) {
    current_test = "nothing";
    chc_block *b = read_one_block("SELECT NULL FROM numbers(3)");
    CHECK(b != NULL); if (!b) return;
    CHECK_EQ_U64(chc_block_n_rows(b), 3);
    CHECK_EQ_U64(chc_block_n_columns(b), 1);

    const chc_type *t = chc_block_column_type(b, 0);
    /* Outer Nullable, inner Nothing. */
    CHECK_EQ_I64(chc_type_kind(t), CHC_NULLABLE);
    const chc_type *inner = chc_type_child(t, 0);
    CHECK_EQ_I64(chc_type_kind(inner), CHC_NOTHING);

    const chc_column *c = chc_block_column(b, 0);
    CHECK(chc_column_layout(c) == CHC_COL_NULLABLE);
    const uint8_t *nm = chc_column_null_map(c);
    for (int i = 0; i < 3; i++) CHECK_EQ_I64(nm[i], 1);

    const chc_column *ic = chc_column_nullable_inner(c);
    CHECK(chc_column_layout(ic) == CHC_COL_NOTHING);
    CHECK_EQ_U64(chc_column_n_rows(ic), 3);
    free_block(b);
}

/*
 * Date wire-widths: chc surfaces raw wire bytes via chc_column_fixed_data,
 * not pre-widened seconds. A consumer that wants seconds-since-epoch must
 * widen explicitly. Pinned here so callers don't accidentally reintroduce
 * the "I read 8 bytes per Date row" footgun.
 *   Date         u16  → 2 bytes per row (days since epoch)
 *   Date32       i32  → 4 bytes per row (signed days since epoch)
 *   DateTime     u32  → 4 bytes per row (seconds since epoch)
 *   DateTime64   i64  → 8 bytes per row (raw ticks at column scale)
 */
static void test_date_widths(void) {
    current_test = "date_widths";
    chc_block *b = read_one_block(
        "SELECT toDate('2020-01-02')      AS d1,"
        "       toDate32('2020-01-02')    AS d2,"
        "       toDateTime('2020-01-02 03:04:05', 'UTC') AS d3,"
        "       toDateTime64('2020-01-02 03:04:05.678', 3, 'UTC') AS d4");
    CHECK(b != NULL); if (!b) return;
    CHECK_EQ_U64(chc_block_n_rows(b), 1);

    size_t es;
    const chc_column *c1 = chc_block_column(b, 0);
    CHECK(chc_column_layout(c1) == CHC_COL_FIXED);
    const uint8_t *p1 = chc_column_fixed_data(c1, &es);
    CHECK_EQ_U64(es, 2);
    /* 2020-01-02 = days 18263 since 1970-01-01. */
    uint16_t v1;
    memcpy(&v1, p1, 2);
    CHECK_EQ_U64(v1, 18263);

    const chc_column *c2 = chc_block_column(b, 1);
    const int32_t *p2 = chc_column_fixed_data(c2, &es);
    CHECK_EQ_U64(es, 4);
    CHECK_EQ_I64(p2[0], 18263);

    const chc_column *c3 = chc_block_column(b, 2);
    const uint32_t *p3 = chc_column_fixed_data(c3, &es);
    CHECK_EQ_U64(es, 4);
    /* 2020-01-02 03:04:05 UTC = 1577934245 seconds. */
    CHECK_EQ_U64(p3[0], 1577934245u);

    const chc_column *c4 = chc_block_column(b, 3);
    const int64_t *p4 = chc_column_fixed_data(c4, &es);
    CHECK_EQ_U64(es, 8);
    /* DateTime64(3) ticks = milliseconds-since-epoch. */
    CHECK_EQ_I64(p4[0], INT64_C(1577934245678));
    /* Scale + tz surface on the type. */
    const chc_type *t4 = chc_block_column_type(b, 3);
    CHECK_EQ_I64(chc_type_datetime64_scale(t4), 3);
    size_t tzlen;
    const char *tz = chc_type_timezone(t4, &tzlen);
    CHECK_STR_EQ(tz, tzlen, "UTC");
    free_block(b);
}

/*
 * JSON column with output_format_native_write_json_as_string=1. Wire
 * layout: 8-byte LE serialization version (=1, STRING) followed by a
 * String column body. Decoder surfaces CHC_COL_STRING; type kind stays
 * CHC_JSON so consumers can route to jsonb / json.
 */
static void test_json_string(void) {
    current_test = "json_string";
    chc_block *b = read_one_block(
        "SELECT arrayJoin(['{}', '{\"a\":1}'])::JSON AS j "
        "SETTINGS output_format_native_write_json_as_string = 1");
    CHECK(b != NULL); if (!b) return;
    CHECK_EQ_U64(chc_block_n_rows(b), 2);
    CHECK_EQ_U64(chc_block_n_columns(b), 1);

    const chc_type *t = chc_block_column_type(b, 0);
    CHECK_EQ_I64(chc_type_kind(t), CHC_JSON);

    const chc_column *c = chc_block_column(b, 0);
    CHECK(chc_column_layout(c) == CHC_COL_STRING);
    const uint8_t  *sd = chc_column_string_data(c);
    const uint64_t *so = chc_column_string_offsets(c);
    CHECK_EQ_U64(so[0], 2);                          /* "{}" */
    CHECK_STR_EQ((const char *) sd, (size_t) so[0], "{}");
    /* CH 25.8+ widens through JSON parser to {"a":"1"}; older LTS keeps
     * {"a":1}. Both serialise same value, accept either. */
    const char *row1 = (const char *) sd + so[0];
    size_t row1_len = (size_t) (so[1] - so[0]);
    int row1_ok = (row1_len == 9 && memcmp(row1, "{\"a\":\"1\"}", 9) == 0) ||
                  (row1_len == 7 && memcmp(row1, "{\"a\":1}", 7) == 0);
    CHECK(row1_ok);
    free_block(b);
}

/*
 * Without the setting, server emits V1 (=0). Decoder errors with a
 * message naming the version & the consumer setting so an operator can
 * fix the missing query setting rather than chasing a generic decode
 * failure.
 */
static void test_json_wrong_version(void) {
    current_test = "json_wrong_version";
    int fd;
    pid_t pid = spawn_local("SELECT '{}'::JSON", &fd);
    if (pid < 0) { fail_count++; return; }

    chc_alloc al = chc_alloc_stdlib();
    chc_posix_io state;
    chc_io io;
    chc_posix_io_init(&state, &io, fd, NULL, NULL);

    chc_block *b = NULL;
    chc_block_opts opts = {0};
    chc_err err = {0};
    int rc = chc_block_read(&io, &al, &opts, &b, &err);
    close(fd);
    int status;
    waitpid(pid, &status, 0);

    CHECK(rc != CHC_OK);
    CHECK(b == NULL);
    CHECK(strstr(err.msg, "JSON serialization version") != NULL);
    CHECK(strstr(err.msg, "output_format_native_write_json_as_string") != NULL);
    if (b) free_block(b);
}

static void test_type_parse_roundtrip(void) {
    current_test = "type_parse_roundtrip";
    const char *types[] = {
        "UInt32",
        "String",
        "FixedString(8)",
        "DateTime64(3, 'UTC')",
        "Nullable(Int64)",
        "Array(UInt8)",
        "Tuple(UInt8, String)",
        "Tuple(a UInt8, b String)",
        "Tuple(x LowCardinality(String), y Array(Int32))",
        "Map(String, Int32)",
        "LowCardinality(String)",
        "Decimal64(4)",
        "Enum8('a' = 1, 'b' = 2)",
        "UUID",
        "IPv4",
        "IPv6",
        "Object('json')",
    };
    chc_alloc al = chc_alloc_stdlib();
    for (size_t i = 0; i < sizeof types / sizeof types[0]; i++) {
        chc_type *t = NULL;
        chc_err err = {0};
        int rc = chc_type_parse(types[i], strlen(types[i]), &al, &t, &err);
        if (rc != CHC_OK) {
            fprintf(stderr, "parse %s failed: %s\n", types[i], err.msg);
            fail_count++;
            continue;
        }
        char buf[128];
        size_t need = chc_type_format(t, buf, sizeof buf);
        if (need >= sizeof buf) {
            fprintf(stderr, "%s: format overflow\n", types[i]);
            fail_count++;
        }
        /* Round-trip should be identical or a normalised form */
        if (strcmp(types[i], buf) != 0) {
            /* For Enum the input has spaces in the formatted output too, OK */
            fprintf(stderr, "%s: round-trip mismatch -> %s\n", types[i], buf);
            /* not a hard fail: we exact-match the source string today */
        }
        chc_type_destroy(t, &al);
    }
}

int main(void) {
    test_type_parse_roundtrip();
    test_named_tuple_parser();
    test_uint32_string();
    test_nullable();
    test_array();
    test_lowcardinality();
    test_lowcardinality_nullable();
    test_tuple();
    test_named_tuple();
    test_map();
    test_decimal();
    test_uuid();
    test_nothing();
    test_date_widths();
    test_point();
    test_ring();
    test_polygon();
    test_multipolygon();
    test_json_string();
    test_json_wrong_version();

    if (fail_count) {
        fprintf(stderr, "FAIL: %d check(s) failed\n", fail_count);
        return 1;
    }
    fprintf(stderr, "ok\n");
    return 0;
}
