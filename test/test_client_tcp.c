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

#define TEMP_ROOT_DIR "/tmp/ch-test-srv"
#define TEST_PORT     19000
#define STR_(x)       #x
#define STR(x)        STR_(x)

static pid_t g_server_pid = -1;

static int
connect_to_server(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons((uint16_t) TEST_PORT);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    for (int i = 0; i < 30; i++) {
        if (connect(fd, (struct sockaddr *) &sa, sizeof sa) == 0) return fd;
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 200 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    close(fd);
    return -1;
}

/* Write `path` with `body` if it does not already exist. */
static int
write_if_absent(const char *path, const char *body)
{
    struct stat st;
    if (stat(path, &st) == 0) return 0;
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fputs(body, f);
    return fclose(f);
}

/* Spawn clickhouse-server using an inline minimal config under
 * /tmp/ch-test-srv. Every runtime path is pinned via command-line flags
 * & the child cwds into the server root so preprocessed_configs/ &
 * access/ never land in the caller's working directory. Returns pid or
 * -1. */
static pid_t
start_server(void)
{
    mkdir(TEMP_ROOT_DIR,              0700);
    mkdir(TEMP_ROOT_DIR "/data",       0700);
    mkdir(TEMP_ROOT_DIR "/tmp",        0700);
    mkdir(TEMP_ROOT_DIR "/user_files", 0700);


    /* Minimal config: every path & port is supplied on the command line
     * so the file body only needs to point at users.xml & silence the
     * console logger. */
    if (write_if_absent(TEMP_ROOT_DIR "/config.xml",
        "<clickhouse>\n"
        "  <logger>\n"
        "    <level>warning</level>\n"
        "    <console>0</console>\n"
        "  </logger>\n"
        "  <users_config>users.xml</users_config>\n"
        "  <default_profile>default</default_profile>\n"
        "  <default_database>default</default_database>\n"
        "  <mark_cache_size>5368709120</mark_cache_size>\n"
        "</clickhouse>\n") != 0) return -1;

    if (write_if_absent(TEMP_ROOT_DIR "/users.xml",
        "<clickhouse>\n"
        "  <profiles><default><load_balancing>random</load_balancing></default></profiles>\n"
        "  <users><default>\n"
        "    <password></password>\n"
        "    <networks><ip>::/0</ip></networks>\n"
        "    <profile>default</profile>\n"
        "    <quota>default</quota>\n"
        "    <access_management>1</access_management>\n"
        "  </default></users>\n"
        "  <quotas><default/></quotas>\n"
        "</clickhouse>\n") != 0) return -1;

    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        if (chdir(TEMP_ROOT_DIR) != 0) _exit(127);
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) { dup2(devnull, STDOUT_FILENO); dup2(devnull, STDERR_FILENO); close(devnull); }
        execlp("clickhouse", "clickhouse", "server",
               "--config-file", "config.xml",
               "--",
               "--tcp_port=" STR(TEST_PORT),
               "--http_port=0",
               "--interserver_http_port=0",
               "--mysql_port=0",
               "--postgresql_port=0",
               "--listen_host=127.0.0.1",
               "--path=data/",
               "--tmp_path=tmp/",
               "--user_files_path=user_files/",
               "--logger.log=server.log",
               "--logger.errorlog=server.err",
               "--logger.level=warning",
               (char *) NULL);
        _exit(127);
    }
    /* Wait for port. */
    for (int i = 0; i < 60; i++) {
        int probe = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in sa = {0};
        sa.sin_family = AF_INET;
        sa.sin_port = htons((uint16_t) TEST_PORT);
        sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        int rc = connect(probe, (struct sockaddr *) &sa, sizeof sa);
        close(probe);
        if (rc == 0) return pid;
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 500 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    /* Reap dead child if it never came up. */
    int status;
    if (waitpid(pid, &status, WNOHANG) == pid) {
        fprintf(stderr, "clickhouse-server died during startup (status=%d)\n", status);
    }
    return -1;
}

static void
stop_server(void)
{
    if (g_server_pid <= 0) return;
    kill(g_server_pid, SIGTERM);
    for (int i = 0; i < 50; i++) {
        int status;
        pid_t r = waitpid(g_server_pid, &status, WNOHANG);
        if (r == g_server_pid) { g_server_pid = -1; return; }
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 100 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    kill(g_server_pid, SIGKILL);
    waitpid(g_server_pid, NULL, 0);
    g_server_pid = -1;
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
}

/* ------------------------------------------------------------------ */

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
    t.al = chc_alloc_stdlib();
    t.fd = connect_to_server();
    CHECK(t.fd >= 0);
    chc_posix_io_init(&t.io_state, &t.io, t.fd, NULL, NULL);
    chc_client_opts opts = { .database = "no such database" };
    int rc = chc_client_init(&t.c, &opts, &t.al, &t.io, &err);
    CHECK(rc == CHC_ERR_SERVER);
    CHECK(strstr(err.msg, "Database") != NULL);
    CHECK(strstr(err.msg, "no such database") != NULL);
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
    rc = chc_client_send_query(t.c, sql, strlen(sql), "", 0, &err);
    CHECK_OK(rc, err);

    bool saw_data = false, saw_eos = false;
    for (int i = 0; i < 32 && !saw_eos; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err);
        CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA && pkt.block
            && chc_block_n_rows(pkt.block) > 0) {
            saw_data = true;
            CHECK(chc_block_n_columns(pkt.block) == 1);
            const chc_column *col = chc_block_column(pkt.block, 0);
            const chc_type *ty = chc_block_column_type(pkt.block, 0);
            CHECK(chc_column_layout(col) == CHC_COL_FIXED);
            size_t elem_size = 0;
            const void *data = chc_column_fixed_data(col, &elem_size);
            CHECK(data != NULL);
            /* `SELECT 42` returns a UInt8 column. */
            (void) ty;
            CHECK(elem_size >= 1);
            CHECK(((const uint8_t *) data)[0] == 42);
        }
        if (pkt.kind == CHC_PKT_END_OF_STREAM) saw_eos = true;
        if (pkt.kind == CHC_PKT_EXCEPTION) {
            fprintf(stderr, "%s: server exception: %s\n",
                    current_test, pkt.exception ? pkt.exception->display_text : "");
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(saw_data);
    CHECK(saw_eos);
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
    rc = chc_client_send_query(t.c, sql, strlen(sql), "", 0, &err);
    CHECK_OK(rc, err);

    size_t total_rows = 0;
    uint64_t sum = 0;
    bool saw_eos = false;
    for (int i = 0; i < 64 && !saw_eos; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err);
        CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA && pkt.block
            && chc_block_n_rows(pkt.block) > 0) {
            CHECK(chc_block_n_columns(pkt.block) == 2);
            size_t nrows = chc_block_n_rows(pkt.block);
            total_rows += nrows;
            const chc_column *c0 = chc_block_column(pkt.block, 0);
            size_t es = 0;
            const uint64_t *vals = chc_column_fixed_data(c0, &es);
            CHECK(es == 8);
            for (size_t r = 0; r < nrows; r++) sum += vals[r];
            const chc_column *c1 = chc_block_column(pkt.block, 1);
            CHECK(chc_column_layout(c1) == CHC_COL_STRING);
            const uint64_t *off = chc_column_string_offsets(c1);
            const uint8_t  *bytes = chc_column_string_data(c1);
            CHECK(off != NULL && bytes != NULL);
        }
        if (pkt.kind == CHC_PKT_END_OF_STREAM) saw_eos = true;
        if (pkt.kind == CHC_PKT_EXCEPTION) {
            fprintf(stderr, "%s: server exception: %s\n",
                    current_test, pkt.exception ? pkt.exception->display_text : "");
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(saw_eos);
    CHECK(total_rows == 100);
    /* sum 0..99 = 4950 */
    CHECK(sum == 4950);
out:
    close_conn(&t);
}

static int
run_simple_query(test_conn *t, const char *sql, chc_err *err)
{
    int rc = chc_client_send_query(t->c, sql, strlen(sql), "", 0, err);
    if (rc != CHC_OK) return rc;
    for (int i = 0; i < 64; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t->c, &pkt, err);
        if (rc != CHC_OK) { chc_packet_clear(t->c, &pkt); return rc; }
        bool done = (pkt.kind == CHC_PKT_END_OF_STREAM);
        bool exc  = (pkt.kind == CHC_PKT_EXCEPTION);
        if (exc && pkt.exception) {
            chc__err_set(err, CHC_ERR_SERVER, "%s", pkt.exception->display_text);
            err->server_code = pkt.exception->code;
        }
        chc_packet_clear(t->c, &pkt);
        if (done) return CHC_OK;
        if (exc) return CHC_ERR_SERVER;
    }
    return chc__err_set(err, CHC_ERR_PROTOCOL, "too many packets");
}

static void
test_exception(void)
{
    current_test = "exception";
    test_conn t; chc_err err = {0};
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    const char *sql = "SELECT * FROM nonexistent_table_xyz";
    rc = chc_client_send_query(t.c, sql, strlen(sql), "", 0, &err);
    CHECK_OK(rc, err);

    bool saw_exc = false;
    for (int i = 0; i < 32; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
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
        if (pkt.kind == CHC_PKT_END_OF_STREAM) break;
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
    int rc = open_conn(&t, &err); CHECK_OK(rc, err);

    /* Drop+create a Memory table (Memory engine doesn't persist across
     * server restarts but lives for this run). */
    rc = run_simple_query(&t, "DROP TABLE IF EXISTS test_t SYNC", &err);
    CHECK_OK(rc, err);
    rc = run_simple_query(&t,
        "CREATE TABLE test_t (x UInt32, s String) ENGINE = Memory", &err);
    CHECK_OK(rc, err);

    /* INSERT: send query, wait for empty Data ("columns echo"), send block,
     * send empty terminator, drain to EndOfStream. */
    rc = chc_client_send_query(t.c,
        "INSERT INTO test_t (x, s) VALUES",
        sizeof "INSERT INTO test_t (x, s) VALUES" - 1, "", 0, &err);
    CHECK_OK(rc, err);

    /* Wait for the first Data packet (header w/ schema, 0 rows). */
    bool got_header = false;
    for (int i = 0; i < 8 && !got_header; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA) got_header = true;
        if (pkt.kind == CHC_PKT_EXCEPTION && pkt.exception) {
            fprintf(stderr, "%s: server: %s\n", current_test,
                    pkt.exception->display_text);
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(got_header);

    /* Build a 3-row block. */
    chc_block_builder *bb = NULL;
    rc = chc_block_builder_init(&bb, &t.al, &err); CHECK_OK(rc, err);

    uint32_t xs[3] = { 100, 200, 300 };
    chc_type *u32 = NULL;
    rc = chc_type_parse("UInt32", 6, &t.al, &u32, &err); CHECK_OK(rc, err);
    rc = chc_block_builder_append_fixed(bb, "x", 1, u32, xs, 3, &err);
    CHECK_OK(rc, err);

    uint64_t offs[3] = { 2, 5, 6 };
    const uint8_t bytes[] = "abcdez";   /* "ab", "cde", "z" */
    rc = chc_block_builder_append_string(bb, "s", 1, offs, bytes, 3, &err);
    CHECK_OK(rc, err);

    rc = chc_client_send_data(t.c, bb, &err); CHECK_OK(rc, err);
    /* Empty block terminates INSERT data. */
    rc = chc_client_send_data(t.c, NULL, &err); CHECK_OK(rc, err);

    /* Drain to EndOfStream. */
    bool saw_eos = false;
    for (int i = 0; i < 32 && !saw_eos; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_END_OF_STREAM) saw_eos = true;
        if (pkt.kind == CHC_PKT_EXCEPTION && pkt.exception) {
            fprintf(stderr, "%s: server: %s\n", current_test,
                    pkt.exception->display_text);
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(saw_eos);

    chc_block_builder_destroy(bb);
    chc_type_destroy(u32, &t.al);

    /* Read it back. */
    rc = chc_client_send_query(t.c,
        "SELECT x, s FROM test_t ORDER BY x",
        sizeof "SELECT x, s FROM test_t ORDER BY x" - 1, "", 0, &err);
    CHECK_OK(rc, err);

    uint64_t x_sum = 0;
    size_t n_rows = 0;
    int seen_s = 0;
    saw_eos = false;
    for (int i = 0; i < 64 && !saw_eos; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA && pkt.block
            && chc_block_n_rows(pkt.block) > 0) {
            size_t nrows = chc_block_n_rows(pkt.block);
            n_rows += nrows;
            const chc_column *xc = chc_block_column(pkt.block, 0);
            size_t es = 0;
            const uint32_t *xv = chc_column_fixed_data(xc, &es);
            CHECK(es == 4);
            for (size_t r = 0; r < nrows; r++) x_sum += xv[r];

            const chc_column *sc = chc_block_column(pkt.block, 1);
            const uint64_t *off = chc_column_string_offsets(sc);
            const uint8_t  *by  = chc_column_string_data(sc);
            uint64_t prev = 0;
            for (size_t r = 0; r < nrows; r++) {
                uint64_t end = off[r], len = end - prev;
                if (len == 2 && memcmp(by + prev, "ab",  2) == 0) seen_s |= 1;
                if (len == 3 && memcmp(by + prev, "cde", 3) == 0) seen_s |= 2;
                if (len == 1 && by[prev] == 'z')                  seen_s |= 4;
                prev = end;
            }
        }
        if (pkt.kind == CHC_PKT_END_OF_STREAM) saw_eos = true;
        if (pkt.kind == CHC_PKT_EXCEPTION && pkt.exception) {
            fprintf(stderr, "%s: server: %s\n", current_test,
                    pkt.exception->display_text);
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(n_rows == 3);
    CHECK(x_sum == 600);
    CHECK(seen_s == 7);

    rc = run_simple_query(&t, "DROP TABLE IF EXISTS test_t SYNC", &err);
    CHECK_OK(rc, err);

out:
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

    bool saw_eos = false, saw_value = false;
    for (int i = 0; i < 32 && !saw_eos; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA && pkt.block
            && chc_block_n_rows(pkt.block) > 0) {
            const chc_column *c0 = chc_block_column(pkt.block, 0);
            /* getSetting narrows the underlying type to the smallest that
             * fits, so for 12345 it returns UInt16. Read whatever width
             * came back and widen to compare. */
            size_t es = 0;
            const uint8_t *v = chc_column_fixed_data(c0, &es);
            uint64_t got = 0;
            for (size_t k = 0; k < es; k++)
                got |= ((uint64_t) v[k]) << (8 * k);
            if (got == 12345ULL) saw_value = true;
        }
        if (pkt.kind == CHC_PKT_END_OF_STREAM) saw_eos = true;
        if (pkt.kind == CHC_PKT_EXCEPTION && pkt.exception) {
            fprintf(stderr, "%s: server: %s\n", current_test,
                    pkt.exception->display_text);
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(saw_eos);
    CHECK(saw_value);
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

    bool saw_eos = false, saw_value = false;
    for (int i = 0; i < 32 && !saw_eos; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA && pkt.block
            && chc_block_n_rows(pkt.block) > 0) {
            const chc_column *c0 = chc_block_column(pkt.block, 0);
            const uint64_t *off = chc_column_string_offsets(c0);
            const uint8_t  *by  = chc_column_string_data(c0);
            if (off && by && off[0] == 11
                && memcmp(by, "hello world", 11) == 0) saw_value = true;
        }
        if (pkt.kind == CHC_PKT_END_OF_STREAM) saw_eos = true;
        if (pkt.kind == CHC_PKT_EXCEPTION && pkt.exception) {
            fprintf(stderr, "%s: server: %s\n", current_test,
                    pkt.exception->display_text);
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(saw_eos);
    CHECK(saw_value);
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

    bool saw_eos = false, saw_value = false;
    for (int i = 0; i < 32 && !saw_eos; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA && pkt.block
            && chc_block_n_rows(pkt.block) > 0) {
            /* col 0 = UInt64 length; col 1 = first element. */
            size_t es = 0;
            const uint64_t *lens = chc_column_fixed_data(
                chc_block_column(pkt.block, 0), &es);
            const chc_column *cs = chc_block_column(pkt.block, 1);
            const uint64_t *off = chc_column_string_offsets(cs);
            const uint8_t  *by  = chc_column_string_data(cs);
            if (lens && lens[0] == 2 && off && by
                && off[0] == 3 && memcmp(by, "foo", 3) == 0)
                saw_value = true;
        }
        if (pkt.kind == CHC_PKT_END_OF_STREAM) saw_eos = true;
        if (pkt.kind == CHC_PKT_EXCEPTION && pkt.exception) {
            fprintf(stderr, "%s: server: %s\n", current_test,
                    pkt.exception->display_text);
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(saw_eos);
    CHECK(saw_value);
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
    rc = chc_client_send_query(t.c, sql, strlen(sql), "", 0, &err);
    CHECK_OK(rc, err);

    size_t total_rows = 0;
    uint64_t sum = 0;
    bool saw_eos = false;
    for (int i = 0; i < 256 && !saw_eos; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA && pkt.block
            && chc_block_n_rows(pkt.block) > 0) {
            size_t nrows = chc_block_n_rows(pkt.block);
            total_rows += nrows;
            const chc_column *c0 = chc_block_column(pkt.block, 0);
            size_t es = 0;
            const uint64_t *vals = chc_column_fixed_data(c0, &es);
            CHECK(es == 8);
            for (size_t r = 0; r < nrows; r++) sum += vals[r];
        }
        if (pkt.kind == CHC_PKT_END_OF_STREAM) saw_eos = true;
        if (pkt.kind == CHC_PKT_EXCEPTION) {
            fprintf(stderr, "%s: server exception: %s\n",
                    current_test, pkt.exception ? pkt.exception->display_text : "");
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(saw_eos);
    CHECK(total_rows == 10000);
    /* 0+1+...+9999 = 9999*10000/2 = 49 995 000 */
    CHECK(sum == 49995000ULL);
out:
    close_conn(&t);
}

static void
test_insert_lz4_roundtrip(void)
{
    current_test = "insert_lz4_roundtrip";
    test_conn t; chc_err err = {0};
    int rc = open_conn_compressed(&t, CHC_COMP_LZ4, &err); CHECK_OK(rc, err);

    rc = run_simple_query(&t, "DROP TABLE IF EXISTS test_lz4 SYNC", &err);
    CHECK_OK(rc, err);
    rc = run_simple_query(&t,
        "CREATE TABLE test_lz4 (x UInt32) ENGINE = Memory", &err);
    CHECK_OK(rc, err);

    rc = chc_client_send_query(t.c,
        "INSERT INTO test_lz4 (x) VALUES",
        sizeof "INSERT INTO test_lz4 (x) VALUES" - 1, "", 0, &err);
    CHECK_OK(rc, err);

    bool got_header = false;
    for (int i = 0; i < 8 && !got_header; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA) got_header = true;
        if (pkt.kind == CHC_PKT_EXCEPTION && pkt.exception) {
            fprintf(stderr, "%s: server: %s\n", current_test,
                    pkt.exception->display_text);
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(got_header);

    /* 500 rows -- compresses well, exercises the chunking path. */
    const size_t N = 500;
    uint32_t *xs = malloc(N * sizeof *xs);
    for (size_t i = 0; i < N; i++) xs[i] = (uint32_t) i;

    chc_block_builder *bb = NULL;
    rc = chc_block_builder_init(&bb, &t.al, &err); CHECK_OK(rc, err);
    chc_type *u32 = NULL;
    rc = chc_type_parse("UInt32", 6, &t.al, &u32, &err); CHECK_OK(rc, err);
    rc = chc_block_builder_append_fixed(bb, "x", 1, u32, xs, N, &err);
    CHECK_OK(rc, err);

    rc = chc_client_send_data(t.c, bb, &err); CHECK_OK(rc, err);
    rc = chc_client_send_data(t.c, NULL, &err); CHECK_OK(rc, err);

    bool saw_eos = false;
    for (int i = 0; i < 32 && !saw_eos; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_END_OF_STREAM) saw_eos = true;
        if (pkt.kind == CHC_PKT_EXCEPTION && pkt.exception) {
            fprintf(stderr, "%s: server: %s\n", current_test,
                    pkt.exception->display_text);
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(saw_eos);

    chc_block_builder_destroy(bb);
    chc_type_destroy(u32, &t.al);
    free(xs);

    /* Sum-check via SELECT, also over LZ4. */
    rc = chc_client_send_query(t.c,
        "SELECT count(), sum(x) FROM test_lz4",
        sizeof "SELECT count(), sum(x) FROM test_lz4" - 1, "", 0, &err);
    CHECK_OK(rc, err);

    saw_eos = false;
    uint64_t got_count = 0, got_sum = 0;
    for (int i = 0; i < 32 && !saw_eos; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA && pkt.block
            && chc_block_n_rows(pkt.block) > 0) {
            const chc_column *c0 = chc_block_column(pkt.block, 0);
            const chc_column *c1 = chc_block_column(pkt.block, 1);
            size_t es = 0;
            const uint64_t *cv = chc_column_fixed_data(c0, &es);
            got_count = cv[0];
            cv = chc_column_fixed_data(c1, &es);
            got_sum = cv[0];
        }
        if (pkt.kind == CHC_PKT_END_OF_STREAM) saw_eos = true;
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(got_count == N);
    /* sum 0..499 = 499*500/2 = 124 750 */
    CHECK(got_sum == 124750ULL);

    rc = run_simple_query(&t, "DROP TABLE IF EXISTS test_lz4 SYNC", &err);
    CHECK_OK(rc, err);

out:
    close_conn(&t);
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
    rc = chc_client_send_query(t.c, sql, strlen(sql), "", 0, &err);
    CHECK_OK(rc, err);

    size_t total_rows = 0;
    uint64_t sum = 0;
    bool saw_eos = false;
    for (int i = 0; i < 256 && !saw_eos; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA && pkt.block
            && chc_block_n_rows(pkt.block) > 0) {
            size_t nrows = chc_block_n_rows(pkt.block);
            total_rows += nrows;
            const chc_column *c0 = chc_block_column(pkt.block, 0);
            size_t es = 0;
            const uint64_t *vals = chc_column_fixed_data(c0, &es);
            CHECK(es == 8);
            for (size_t r = 0; r < nrows; r++) sum += vals[r];
        }
        if (pkt.kind == CHC_PKT_END_OF_STREAM) saw_eos = true;
        if (pkt.kind == CHC_PKT_EXCEPTION) {
            fprintf(stderr, "%s: server exception: %s\n",
                    current_test, pkt.exception ? pkt.exception->display_text : "");
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(saw_eos);
    CHECK(total_rows == 10000);
    CHECK(sum == 49995000ULL);
out:
    close_conn(&t);
}

static void
test_insert_zstd_roundtrip(void)
{
    current_test = "insert_zstd_roundtrip";
    test_conn t; chc_err err = {0};
    int rc = open_conn_compressed(&t, CHC_COMP_ZSTD, &err); CHECK_OK(rc, err);

    /* Switch the session over to ZSTD for any server-emitted compressed
     * blocks (schema echo on INSERT, the readback SELECT). */
    rc = run_simple_query(&t,
        "SET network_compression_method='zstd'", &err);
    CHECK_OK(rc, err);

    rc = run_simple_query(&t, "DROP TABLE IF EXISTS test_zstd SYNC", &err);
    CHECK_OK(rc, err);
    rc = run_simple_query(&t,
        "CREATE TABLE test_zstd (x UInt32) ENGINE = Memory", &err);
    CHECK_OK(rc, err);

    rc = chc_client_send_query(t.c,
        "INSERT INTO test_zstd (x) VALUES",
        sizeof "INSERT INTO test_zstd (x) VALUES" - 1, "", 0, &err);
    CHECK_OK(rc, err);

    bool got_header = false;
    for (int i = 0; i < 8 && !got_header; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA) got_header = true;
        if (pkt.kind == CHC_PKT_EXCEPTION && pkt.exception) {
            fprintf(stderr, "%s: server: %s\n", current_test,
                    pkt.exception->display_text);
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(got_header);

    const size_t N = 500;
    uint32_t *xs = malloc(N * sizeof *xs);
    for (size_t i = 0; i < N; i++) xs[i] = (uint32_t) i;

    chc_block_builder *bb = NULL;
    rc = chc_block_builder_init(&bb, &t.al, &err); CHECK_OK(rc, err);
    chc_type *u32 = NULL;
    rc = chc_type_parse("UInt32", 6, &t.al, &u32, &err); CHECK_OK(rc, err);
    rc = chc_block_builder_append_fixed(bb, "x", 1, u32, xs, N, &err);
    CHECK_OK(rc, err);

    rc = chc_client_send_data(t.c, bb, &err); CHECK_OK(rc, err);
    rc = chc_client_send_data(t.c, NULL, &err); CHECK_OK(rc, err);

    bool saw_eos = false;
    for (int i = 0; i < 32 && !saw_eos; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_END_OF_STREAM) saw_eos = true;
        if (pkt.kind == CHC_PKT_EXCEPTION && pkt.exception) {
            fprintf(stderr, "%s: server: %s\n", current_test,
                    pkt.exception->display_text);
            fail_count++;
        }
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(saw_eos);

    chc_block_builder_destroy(bb);
    chc_type_destroy(u32, &t.al);
    free(xs);

    rc = chc_client_send_query(t.c,
        "SELECT count(), sum(x) FROM test_zstd",
        sizeof "SELECT count(), sum(x) FROM test_zstd" - 1, "", 0, &err);
    CHECK_OK(rc, err);

    saw_eos = false;
    uint64_t got_count = 0, got_sum = 0;
    for (int i = 0; i < 32 && !saw_eos; i++) {
        chc_packet pkt = {0};
        rc = chc_client_recv_packet(t.c, &pkt, &err); CHECK_OK(rc, err);
        if (pkt.kind == CHC_PKT_DATA && pkt.block
            && chc_block_n_rows(pkt.block) > 0) {
            const chc_column *c0 = chc_block_column(pkt.block, 0);
            const chc_column *c1 = chc_block_column(pkt.block, 1);
            size_t es = 0;
            const uint64_t *cv = chc_column_fixed_data(c0, &es);
            got_count = cv[0];
            cv = chc_column_fixed_data(c1, &es);
            got_sum = cv[0];
        }
        if (pkt.kind == CHC_PKT_END_OF_STREAM) saw_eos = true;
        chc_packet_clear(t.c, &pkt);
    }
    CHECK(got_count == N);
    CHECK(got_sum == 124750ULL);

    rc = run_simple_query(&t, "DROP TABLE IF EXISTS test_zstd SYNC", &err);
    CHECK_OK(rc, err);

out:
    close_conn(&t);
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

    g_server_pid = start_server();
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
