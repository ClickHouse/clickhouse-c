/*
 * test_client_tcp.c -- end-to-end TCP client tests against a live
 * clickhouse-server. Spawns the server on a fixed loopback port,
 * exercises handshake / Ping / SELECT / INSERT / exception paths, then
 * tears down.
 *
 * Compile:
 *   cc -std=c11 -O2 -I. test/test_client_tcp.c -o /tmp/test_client_tcp
 *
 * Run requires:
 *   - clickhouse binary on PATH (provides both `server` and `local` modes)
 *   - port 19000 free on 127.0.0.1
 */

#include <arpa/inet.h>
#include <assert.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#include "clickhouse-posix-io.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"

static int fail_count = 0;
static const char *current_test = "";

#include "test_common.h"
#include "test_clickhouse_server.h"

#define TEMP_ROOT_DIR "/tmp/ch-test-srv"
#define TEST_PORT     19000

static pid_t g_server_pid = -1;

static int
connect_to_server(void)
{
    return test_clickhouse_connect(TEST_PORT);
}

static void
stop_server(void)
{
    test_clickhouse_server_stop(&g_server_pid);
}

/* Wraps connect_to_server + chc_posix_io_init + chc_client_init. */
typedef struct {
    int fd;
    chc_posix_io io_state;
    chc_io io;
    chc_alloc al;
    chc_client *c;
    chc_codec codec;
} test_conn;

static int
open_conn_compressed(test_conn *t, chc_compression comp, chc_err *err)
{
    memset(t, 0, sizeof *t);
    t->al = chc_alloc_stdlib();
    t->fd = connect_to_server();
    if (t->fd < 0) {
        chc__err_set(err, CHC_ERR_IO, "connect failed");
        return CHC_ERR_IO;
    }
    chc_posix_io_init(&t->io_state, &t->io, t->fd, NULL, NULL);
    chc_client_opts opts = {0};
    if (comp == CHC_COMP_LZ4) {
        chc_lz4_codec_init(&t->codec);
        opts.compression = CHC_COMP_LZ4;
        opts.codec = &t->codec;
    } else if (comp == CHC_COMP_ZSTD) {
        chc_zstd_codec_init(&t->codec);
        opts.compression = CHC_COMP_ZSTD;
        opts.codec = &t->codec;
    }
    int rc = chc_client_init(&t->c, &opts, &t->al, &t->io, err);
    if (rc != CHC_OK) {
        close(t->fd);
        t->fd = -1;
    }
    return rc;
}

static int
open_conn(test_conn *t, chc_err *err)
{
    return open_conn_compressed(t, CHC_COMP_NONE, err);
}

static void
close_conn(test_conn *t)
{
    if (t->c) chc_client_close(t->c);
    if (t->fd >= 0) close(t->fd);
    memset(t, 0, sizeof *t);
    t->fd = -1;
}

/* ------------------------------------------------------------------ */

typedef void (*test_block_cb)(const chc_block *b, void *ud);

static int
send_query(test_conn *t, const char *sql, chc_err *err)
{
    return chc_client_send_query(t->c, sql, strlen(sql), "", 0, err);
}

static int
recv_until_eos(test_conn *t, int max_packets, test_block_cb cb, void *ud,
               chc_err *err)
{
    for (int i = 0; i < max_packets; i++) {
        chc_packet pkt = {0};
        int rc = chc_client_recv_packet(t->c, &pkt, err);
        if (rc != CHC_OK) {
            chc_packet_clear(t->c, &pkt);
            return rc;
        }

        bool done = pkt.kind == CHC_PKT_END_OF_STREAM;
        bool exc = pkt.kind == CHC_PKT_EXCEPTION;
        if (exc && pkt.exception) {
            chc__err_set(err, CHC_ERR_SERVER, "%s", pkt.exception->display_text);
            err->server_code = pkt.exception->code;
        }
        if (pkt.kind == CHC_PKT_DATA && pkt.block &&
            chc_block_n_rows(pkt.block) > 0 && cb) {
            cb(pkt.block, ud);
        }
        chc_packet_clear(t->c, &pkt);
        if (done) return CHC_OK;
        if (exc) return CHC_ERR_SERVER;
    }
    return chc__err_set(err, CHC_ERR_PROTOCOL, "too many packets");
}

static int
recv_insert_header(test_conn *t, int max_packets, chc_err *err)
{
    for (int i = 0; i < max_packets; i++) {
        chc_packet pkt = {0};
        int rc = chc_client_recv_packet(t->c, &pkt, err);
        if (rc != CHC_OK) {
            chc_packet_clear(t->c, &pkt);
            return rc;
        }

        bool got_data = pkt.kind == CHC_PKT_DATA;
        bool exc = pkt.kind == CHC_PKT_EXCEPTION;
        if (exc && pkt.exception) {
            chc__err_set(err, CHC_ERR_SERVER, "%s", pkt.exception->display_text);
            err->server_code = pkt.exception->code;
        }
        chc_packet_clear(t->c, &pkt);
        if (got_data) return CHC_OK;
        if (exc) return CHC_ERR_SERVER;
    }
    return chc__err_set(err, CHC_ERR_PROTOCOL, "missing insert header");
}

typedef struct {
    bool saw_data;
} select_simple_acc;

static void
select_simple_cb(const chc_block *b, void *ud)
{
    select_simple_acc *a = ud;
    a->saw_data = true;
    CHECK(chc_block_n_columns(b) == 1);
    const chc_column *col = chc_block_column(b, 0);
    CHECK(chc_column_layout(col) == CHC_COL_FIXED);
    size_t elem_size = 0;
    const void *data = chc_column_fixed_data(col, &elem_size);
    CHECK(data != NULL);
    CHECK(elem_size >= 1);
    if (data && elem_size >= 1)
        CHECK(((const uint8_t *) data)[0] == 42);
}

typedef struct {
    size_t total_rows;
    uint64_t sum;
    size_t expect_columns;
    bool check_string_col;
} select_many_acc;

static void
select_many_cb(const chc_block *b, void *ud)
{
    select_many_acc *a = ud;
    if (a->expect_columns) CHECK(chc_block_n_columns(b) == a->expect_columns);
    size_t nrows = chc_block_n_rows(b);
    a->total_rows += nrows;
    const chc_column *c0 = chc_block_column(b, 0);
    size_t es = 0;
    const uint64_t *vals = chc_column_fixed_data(c0, &es);
    CHECK(es == 8);
    CHECK(vals != NULL);
    if (!vals) return;
    for (size_t r = 0; r < nrows; r++) a->sum += vals[r];
    if (a->check_string_col) {
        const chc_column *c1 = chc_block_column(b, 1);
        CHECK(chc_column_layout(c1) == CHC_COL_STRING);
        CHECK(chc_column_string_offsets(c1) != NULL);
        CHECK(chc_column_string_data(c1) != NULL);
    }
}

typedef struct {
    bool saw_value;
} setting_acc;

static void
setting_cb(const chc_block *b, void *ud)
{
    setting_acc *a = ud;
    const chc_column *c0 = chc_block_column(b, 0);
    size_t es = 0;
    const uint8_t *v = chc_column_fixed_data(c0, &es);
    CHECK(v != NULL);
    if (!v) return;
    uint64_t got = 0;
    for (size_t k = 0; k < es; k++)
        got |= ((uint64_t) v[k]) << (8 * k);
    if (got == 12345ULL) a->saw_value = true;
}

static void
string_param_cb(const chc_block *b, void *ud)
{
    setting_acc *a = ud;
    const chc_column *c0 = chc_block_column(b, 0);
    const uint64_t *off = chc_column_string_offsets(c0);
    const uint8_t *by = chc_column_string_data(c0);
    if (off && by && off[0] == 11 && memcmp(by, "hello world", 11) == 0)
        a->saw_value = true;
}

static void
array_param_cb(const chc_block *b, void *ud)
{
    setting_acc *a = ud;
    size_t es = 0;
    const uint64_t *lens = chc_column_fixed_data(chc_block_column(b, 0), &es);
    const chc_column *cs = chc_block_column(b, 1);
    const uint64_t *off = chc_column_string_offsets(cs);
    const uint8_t *by = chc_column_string_data(cs);
    if (lens && lens[0] == 2 && off && by &&
        off[0] == 3 && memcmp(by, "foo", 3) == 0)
        a->saw_value = true;
}

typedef struct {
    uint64_t x_sum;
    size_t n_rows;
    int seen_s;
} insert_readback_acc;

static void
insert_readback_cb(const chc_block *b, void *ud)
{
    insert_readback_acc *a = ud;
    size_t nrows = chc_block_n_rows(b);
    a->n_rows += nrows;
    const chc_column *xc = chc_block_column(b, 0);
    size_t es = 0;
    const uint32_t *xv = chc_column_fixed_data(xc, &es);
    CHECK(es == 4);
    CHECK(xv != NULL);
    if (xv) {
        for (size_t r = 0; r < nrows; r++) a->x_sum += xv[r];
    }

    const chc_column *sc = chc_block_column(b, 1);
    const uint64_t *off = chc_column_string_offsets(sc);
    const uint8_t *by = chc_column_string_data(sc);
    CHECK(off != NULL && by != NULL);
    if (!off || !by) return;
    uint64_t prev = 0;
    for (size_t r = 0; r < nrows; r++) {
        uint64_t end = off[r], len = end - prev;
        if (len == 2 && memcmp(by + prev, "ab",  2) == 0) a->seen_s |= 1;
        if (len == 3 && memcmp(by + prev, "cde", 3) == 0) a->seen_s |= 2;
        if (len == 1 && by[prev] == 'z')                  a->seen_s |= 4;
        prev = end;
    }
}

typedef struct {
    uint64_t count;
    uint64_t sum;
} count_sum_acc;

static void
count_sum_cb(const chc_block *b, void *ud)
{
    count_sum_acc *a = ud;
    const chc_column *c0 = chc_block_column(b, 0);
    const chc_column *c1 = chc_block_column(b, 1);
    size_t es = 0;
    const uint64_t *cv = chc_column_fixed_data(c0, &es);
    CHECK(cv != NULL);
    if (cv) a->count = cv[0];
    cv = chc_column_fixed_data(c1, &es);
    CHECK(cv != NULL);
    if (cv) a->sum = cv[0];
}

static void
test_handshake(void)
{
    current_test = "handshake";
    test_conn t; chc_err err = {0};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);
    const chc_server_info *si = chc_client_server_info(t.c);
    CHECK(si != NULL);
    CHECK(strncmp(si->name, "ClickHouse", 10) == 0);
    CHECK(si->revision >= 54400);
    CHECK(si->version_major >= 1);
out:
    close_conn(&t);
}

/* chc_client_init must surface a bad default_database here, not race
 * the caller's first query. Older CH versions defer the database check
 * until after the Addendum read, so the probe Ping at init tail is the
 * point where the server-side exception lands. */
static void
test_bad_database(void)
{
    current_test = "bad_database";
    test_conn t; chc_err err = {0};
    memset(&t, 0, sizeof t);
    t.fd = -1;
    t.al = chc_alloc_stdlib();
    t.fd = connect_to_server();
    CHECK(t.fd >= 0);
    if (t.fd < 0) goto out;
    chc_posix_io_init(&t.io_state, &t.io, t.fd, NULL, NULL);
    chc_client_opts opts = { .database = "no such database" };
    int rc = chc_client_init(&t.c, &opts, &t.al, &t.io, &err);
    CHECK(rc == CHC_ERR_SERVER);
    CHECK(strstr(err.msg, "Database") != NULL);
    CHECK(strstr(err.msg, "no such database") != NULL);
out:
    close_conn(&t);
}

static void
test_ping(void)
{
    current_test = "ping";
    test_conn t; chc_err err = {0};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);
    rc = chc_client_send_ping(t.c, &err); CHECK_OK(rc, err);
    chc_packet pkt = {0};
    rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
    CHECK(pkt.kind == CHC_PKT_PONG);
    chc_packet_clear(t.c, &pkt);
out:
    close_conn(&t);
}

static void
test_select_simple(void)
{
    current_test = "select_simple";
    test_conn t; chc_err err = {0};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    const char *sql = "SELECT 42 AS x";
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    select_simple_acc acc = {0};
    rc = recv_until_eos(&t, 32, select_simple_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.saw_data);
out:
    close_conn(&t);
}

static void
test_select_many(void)
{
    current_test = "select_many";
    test_conn t; chc_err err = {0};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    const char *sql = "SELECT number, toString(number) FROM system.numbers LIMIT 100";
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    select_many_acc acc = {
        .expect_columns = 2,
        .check_string_col = true,
    };
    rc = recv_until_eos(&t, 64, select_many_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.total_rows == 100);
    CHECK(acc.sum == 4950);
out:
    close_conn(&t);
}

static int
run_simple_query(test_conn *t, const char *sql, chc_err *err)
{
    int rc = send_query(t, sql, err);
    if (rc != CHC_OK) return rc;
    return recv_until_eos(t, 64, NULL, NULL, err);
}

static void
test_exception(void)
{
    current_test = "exception";
    test_conn t; chc_err err = {0};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    const char *sql = "SELECT * FROM nonexistent_table_xyz";
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    bool saw_exc = false;
    for (int i = 0; i < 32; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        bool eos = pkt.kind == CHC_PKT_END_OF_STREAM;
        if (pkt.kind == CHC_PKT_EXCEPTION) {
            saw_exc = true;
            CHECK(pkt.exception != NULL);
            /* Error code 60 = UNKNOWN_TABLE */
            CHECK(pkt.exception->code == 60 || pkt.exception->code == 81
                  || pkt.exception->code == 47);
            CHECK(pkt.exception->name && strstr(pkt.exception->name, "DB::") != NULL);
            chc_packet_clear(t.c, &pkt);
            break;
        }
        chc_packet_clear(t.c, &pkt);
        if (eos) break;
    }
    CHECK(saw_exc);
out:
    close_conn(&t);
}

static void
test_insert_roundtrip(void)
{
    current_test = "insert_roundtrip";
    test_conn t; chc_err err = {0};
    chc_block_builder *bb = NULL;
    chc_type *u32 = NULL;
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    /* Drop+create a Memory table (Memory engine doesn't persist across
     * server restarts but lives for this run). */
    rc = run_simple_query(&t, "DROP TABLE IF EXISTS test_t SYNC", &err);
    CHECK_OK(rc, err);
    rc = run_simple_query(&t,
        "CREATE TABLE test_t (x UInt32, s String) ENGINE = Memory", &err);
    CHECK_OK(rc, err);

    rc = send_query(&t, "INSERT INTO test_t (x, s) VALUES", &err);
    CHECK_OK(rc, err);

    rc = recv_insert_header(&t, 8, &err);
    CHECK_OK(rc, err);

    /* Build a 3-row block. */
    rc = chc_block_builder_init(&bb, &t.al, &err); CHECK_OK(rc, err);

    uint32_t xs[3] = { 100, 200, 300 };
    rc = chc_type_parse("UInt32", 6, &t.al, &u32, &err); CHECK_OK(rc, err);
    rc = chc_block_builder_append_fixed(bb, "x", 1, u32, xs, 3, &err);
    CHECK_OK(rc, err);

    uint64_t offs[3] = { 2, 5, 6 };
    const uint8_t bytes[] = "abcdez";   /* "ab", "cde", "z" */
    rc = chc_block_builder_append_string(bb, "s", 1, offs, bytes, 3, &err);
    CHECK_OK(rc, err);

    rc = chc_client_send_data(t.c, bb, &err); CHECK_OK(rc, err);
    rc = chc_client_send_data(t.c, NULL, &err); CHECK_OK(rc, err);

    rc = recv_until_eos(&t, 32, NULL, NULL, &err);
    CHECK_OK(rc, err);

    chc_block_builder_destroy(bb);
    bb = NULL;
    chc_type_destroy(u32, &t.al);
    u32 = NULL;

    /* Read it back. */
    rc = send_query(&t, "SELECT x, s FROM test_t ORDER BY x", &err);
    CHECK_OK(rc, err);

    insert_readback_acc acc = {0};
    rc = recv_until_eos(&t, 64, insert_readback_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.n_rows == 3);
    CHECK(acc.x_sum == 600);
    CHECK(acc.seen_s == 7);

    rc = run_simple_query(&t, "DROP TABLE IF EXISTS test_t SYNC", &err);
    CHECK_OK(rc, err);

out:
    chc_block_builder_destroy(bb);
    chc_type_destroy(u32, &t.al);
    close_conn(&t);
}

static void
test_select_with_setting(void)
{
    current_test = "select_with_setting";
    test_conn t; chc_err err = {0};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    chc_query_setting settings[1] = {
        { .name = "max_block_size", .value = "12345", .important = true },
    };
    chc_query_opts opts = { .settings = settings, .n_settings = 1 };

    const char *sql = "SELECT getSetting('max_block_size')";
    rc = chc_client_send_query_ex(t.c, sql, strlen(sql), &opts, &err);
    CHECK_OK(rc, err);

    setting_acc acc = {0};
    rc = recv_until_eos(&t, 32, setting_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.saw_value);
out:
    close_conn(&t);
}

static void
test_select_with_param(void)
{
    current_test = "select_with_param";
    test_conn t; chc_err err = {0};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    /* Param values pass through Field::restoreFromDump on the server, so
     * a String must arrive single-quoted (matches clickhouse-cpp). */
    chc_query_param params[1] = {
        { .name = "p1", .value = "'hello world'" },
    };
    chc_query_opts opts = { .params = params, .n_params = 1 };

    const char *sql = "SELECT {p1:String} AS s";
    rc = chc_client_send_query_ex(t.c, sql, strlen(sql), &opts, &err);
    CHECK_OK(rc, err);

    setting_acc acc = {0};
    rc = recv_until_eos(&t, 32, string_param_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.saw_value);
out:
    close_conn(&t);
}

/*
 * Array(String) parameter with inner quotes. clickhouse-cpp's
 * Client::SetParam wraps the value in single quotes & escapes inner
 * `'` to `\x27`; chc passes the value verbatim per the doc on
 * chc_query_param. A caller that does NOT escape inner quotes ends up
 * with the server stopping at the first `'` inside the array literal,
 * surfacing as "value [ cannot be parsed as Array(String) for query
 * parameter 'p1'". This test pins both halves: the verbatim
 * pass-through, AND the fact that the server actually parses the
 * escaped form back to a real Array(String).
 */
static void
test_select_with_array_param(void)
{
    current_test = "select_with_array_param";
    test_conn t; chc_err err = {0};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    /* Pre-escaped Array(String) literal: outer quote, inner quotes as
     * \x27, comma between elements. Matches what a caller mirroring
     * clickhouse-cpp's WriteQuotedString would produce. */
    chc_query_param params[1] = {
        { .name = "p1",
          .value = "'[\\x27foo\\x27,\\x27bar\\x27]'" },
    };
    chc_query_opts opts = { .params = params, .n_params = 1 };

    const char *sql =
        "SELECT length({p1:Array(String)}) AS n, {p1:Array(String)}[1] AS s";
    rc = chc_client_send_query_ex(t.c, sql, strlen(sql), &opts, &err);
    CHECK_OK(rc, err);

    setting_acc acc = {0};
    rc = recv_until_eos(&t, 32, array_param_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.saw_value);
out:
    close_conn(&t);
}

static void
test_select_many_lz4(void)
{
    current_test = "select_many_lz4";
    test_conn t; chc_err err = {0};
    int rc = open_conn_compressed(&t, CHC_COMP_LZ4, &err); CHECK_OK(rc, err);

    /* 10 000 rows pushes the body past one LZ4 frame. */
    const char *sql =
        "SELECT number, toString(number) FROM system.numbers LIMIT 10000";
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    select_many_acc acc = {0};
    rc = recv_until_eos(&t, 256, select_many_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.total_rows == 10000);
    CHECK(acc.sum == 49995000ULL);
out:
    close_conn(&t);
}

static void
run_insert_compressed_roundtrip(chc_compression comp, const char *table,
                                bool set_zstd)
{
    test_conn t;
    chc_err err = {0};
    uint32_t *xs = NULL;
    chc_block_builder *bb = NULL;
    chc_type *u32 = NULL;
    const size_t N = 500;
    char sql[160];

    int rc = open_conn_compressed(&t, comp, &err); CHECK_OK(rc, err);

    if (set_zstd) {
        rc = run_simple_query(&t, "SET network_compression_method='zstd'", &err);
        CHECK_OK(rc, err);
    }

    int sn = snprintf(sql, sizeof sql, "DROP TABLE IF EXISTS %s SYNC", table);
    CHECK(sn > 0 && (size_t) sn < sizeof sql);
    if (sn <= 0 || (size_t) sn >= sizeof sql) goto out;
    rc = run_simple_query(&t, sql, &err);
    CHECK_OK(rc, err);

    sn = snprintf(sql, sizeof sql,
                  "CREATE TABLE %s (x UInt32) ENGINE = Memory", table);
    CHECK(sn > 0 && (size_t) sn < sizeof sql);
    if (sn <= 0 || (size_t) sn >= sizeof sql) goto out;
    rc = run_simple_query(&t, sql, &err);
    CHECK_OK(rc, err);

    sn = snprintf(sql, sizeof sql, "INSERT INTO %s (x) VALUES", table);
    CHECK(sn > 0 && (size_t) sn < sizeof sql);
    if (sn <= 0 || (size_t) sn >= sizeof sql) goto out;
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    rc = recv_insert_header(&t, 8, &err);
    CHECK_OK(rc, err);

    xs = malloc(N * sizeof *xs);
    CHECK(xs != NULL);
    if (!xs) goto out;
    for (size_t i = 0; i < N; i++) xs[i] = (uint32_t) i;

    rc = chc_block_builder_init(&bb, &t.al, &err); CHECK_OK(rc, err);
    rc = chc_type_parse("UInt32", 6, &t.al, &u32, &err); CHECK_OK(rc, err);
    rc = chc_block_builder_append_fixed(bb, "x", 1, u32, xs, N, &err);
    CHECK_OK(rc, err);

    rc = chc_client_send_data(t.c, bb, &err); CHECK_OK(rc, err);
    rc = chc_client_send_data(t.c, NULL, &err); CHECK_OK(rc, err);

    rc = recv_until_eos(&t, 32, NULL, NULL, &err);
    CHECK_OK(rc, err);

    chc_block_builder_destroy(bb);
    bb = NULL;
    chc_type_destroy(u32, &t.al);
    u32 = NULL;
    free(xs);
    xs = NULL;

    sn = snprintf(sql, sizeof sql, "SELECT count(), sum(x) FROM %s", table);
    CHECK(sn > 0 && (size_t) sn < sizeof sql);
    if (sn <= 0 || (size_t) sn >= sizeof sql) goto out;
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    count_sum_acc acc = {0};
    rc = recv_until_eos(&t, 32, count_sum_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.count == N);
    CHECK(acc.sum == 124750ULL);

    sn = snprintf(sql, sizeof sql, "DROP TABLE IF EXISTS %s SYNC", table);
    CHECK(sn > 0 && (size_t) sn < sizeof sql);
    if (sn <= 0 || (size_t) sn >= sizeof sql) goto out;
    rc = run_simple_query(&t, sql, &err);
    CHECK_OK(rc, err);

out:
    chc_block_builder_destroy(bb);
    chc_type_destroy(u32, &t.al);
    free(xs);
    close_conn(&t);
}

static void
test_insert_lz4_roundtrip(void)
{
    current_test = "insert_lz4_roundtrip";
    run_insert_compressed_roundtrip(CHC_COMP_LZ4, "test_lz4", false);
}

static void
test_select_many_zstd(void)
{
    current_test = "select_many_zstd";
    test_conn t; chc_err err = {0};
    int rc = open_conn_compressed(&t, CHC_COMP_ZSTD, &err); CHECK_OK(rc, err);

    /* Server defaults to LZ4 for network responses; pin it to ZSTD so the
     * codec's zstd_decompress path is the one exercised. */
    rc = run_simple_query(&t,
        "SET network_compression_method='zstd'", &err);
    CHECK_OK(rc, err);

    /* 10 000 rows: mirrors test_select_many_lz4 so the zstd path is
     * exercised against the same shape of payload. */
    const char *sql =
        "SELECT number, toString(number) FROM system.numbers LIMIT 10000";
    rc = send_query(&t, sql, &err);
    CHECK_OK(rc, err);

    select_many_acc acc = {0};
    rc = recv_until_eos(&t, 256, select_many_cb, &acc, &err);
    CHECK_OK(rc, err);
    CHECK(acc.total_rows == 10000);
    CHECK(acc.sum == 49995000ULL);
out:
    close_conn(&t);
}

static void
test_insert_zstd_roundtrip(void)
{
    current_test = "insert_zstd_roundtrip";
    run_insert_compressed_roundtrip(CHC_COMP_ZSTD, "test_zstd", true);
}

static void
test_cityhash_known_vector(void)
{
    current_test = "cityhash_known_vector";
    /* Spot-check CityHash128 against the upstream test vector for an
     * empty string and a short fixed input. Reference values produced
     * via clickhouse-cpp's CityHash128 on the same inputs. */
    uint64_t lo, hi;
    chc_cityhash128("", 0, &lo, &hi);
    CHECK(lo == 0x3df09dfc64c09a2bULL);
    CHECK(hi == 0x3cb540c392e51e29ULL);

    chc_cityhash128("Hello, World!", 13, &lo, &hi);
    CHECK(lo == 0x703dabf8d081ec00ULL);
    CHECK(hi == 0xa196e28f28c3ee09ULL);

    const char *fox = "The quick brown fox jumps over the lazy dog";
    chc_cityhash128(fox, strlen(fox), &lo, &hi);
    CHECK(lo == 0x69102202d326a2fdULL);
    CHECK(hi == 0xe4b1346bbee531a1ULL);

    /* 200 bytes of 0xab -- exercises the len>=128 unrolled path. */
    char big[200]; memset(big, 0xab, sizeof big);
    chc_cityhash128(big, sizeof big, &lo, &hi);
    CHECK(lo == 0xb29e1d196fe650dfULL);
    CHECK(hi == 0x3dae10d6a77e0432ULL);
}

int main(void)
{
    signal(SIGPIPE, SIG_IGN);

    test_cityhash_known_vector();

    g_server_pid = test_clickhouse_server_start(TEMP_ROOT_DIR, TEST_PORT);
    if (g_server_pid < 0) {
        fprintf(stderr, "failed to start clickhouse-server; skipping tests\n");
        return 77;   /* automake-style "skipped" */
    }
    atexit(stop_server);

    test_handshake();
    test_bad_database();
    test_ping();
    test_select_simple();
    test_select_many();
    test_exception();
    test_insert_roundtrip();
    test_select_with_setting();
    test_select_with_param();
    test_select_with_array_param();
    test_select_many_lz4();
    test_insert_lz4_roundtrip();
    test_select_many_zstd();
    test_insert_zstd_roundtrip();

    stop_server();

    if (fail_count) {
        fprintf(stderr, "FAIL: %d check(s)\n", fail_count);
        return 1;
    }
    fprintf(stderr, "ok\n");
    return 0;
}
