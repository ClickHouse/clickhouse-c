/*
 * test_client_errors.c -- handshake, send & recv rejection paths of
 * clickhouse-client.h plus the async wrappers, driven over hand-seeded
 * chc_client state and memory transports. No server.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#define CHC_NO_ZSTD                 /* lz4 alone covers the compressed paths */
#include "clickhouse.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"
#include "clickhouse-async.h"

static int fail_count = 0;
static const char *current_test = "";

#include "test_common.h"

#define REV_MODERN CHC_CLIENT_DEFAULT_REVISION

/* ---------------- byte builder ------------------------------------------- */

typedef struct { uint8_t d[4096]; size_t n; } wbuf;

static void w8(wbuf *b, uint8_t v) { b->d[b->n++] = v; }

static void
wvar(wbuf *b, uint64_t v)
{
    while (v >= 0x80) { w8(b, (uint8_t) (v | 0x80)); v >>= 7; }
    w8(b, (uint8_t) v);
}

static void
wstr(wbuf *b, const char *s)
{
    size_t n = strlen(s);
    wvar(b, n);
    memcpy(b->d + b->n, s, n);
    b->n += n;
}

static void
wbytes(wbuf *b, const void *p, size_t n)
{
    memcpy(b->d + b->n, p, n);
    b->n += n;
}

/* Server-side exception frame: code, name, text, stack, has_nested. */
static void
wexception(wbuf *b)
{
    uint8_t code[4] = { 42, 0, 0, 0 };
    wbytes(b, code, 4);
    wstr(b, "DB::Exception");
    wstr(b, "boom");
    wstr(b, "0. boom()");
    w8(b, 0);
}

/* ---------------- hand-seeded client ------------------------------------- */

typedef struct {
    chc_client    c;
    test_mem_src  src;
    chc_io        rio;
    test_mem_sink sink;
    chc_io        wio;
} fake;

static int
fake_up(fake *f, const chc_alloc *al, const wbuf *b, uint64_t revision,
        chc_err *err)
{
    memset(f, 0, sizeof *f);
    test_mem_src_init(&f->src, &f->rio, b->d, b->n);
    test_mem_sink_init(&f->sink, &f->wio);
    f->c.al = al;
    f->c.io = &f->wio;
    f->c.compression = CHC_COMP_NONE;
    f->c.client_revision = revision;
    f->c.server.revision = revision;
    return chc_in_init(&f->c.in, &f->rio, al, 0, err);
}

static void
fake_down(fake *f)
{
    chc__client_recv_state_free(&f->c);
    chc_in_free(&f->c.in);
    test_mem_sink_free(&f->sink);
}

/* ---------------- handshake ---------------------------------------------- */

static void
test_handshake_rejects(void)
{
    current_test = "handshake_rejects";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    fake f;
    wbuf b;

    b = (wbuf) {}; wvar(&b, CHC_PKT_PONG);
    if (fake_up(&f, &al, &b, REV_MODERN, &err) == CHC_OK) {
        CHECK(chc__client_recv_hello(&f.c, NULL, &err) == CHC_ERR_PROTOCOL);
        CHECK(strstr(err.msg, "expected Hello") != NULL);
        fake_down(&f);
    } else
        fail_count++;

    b = (wbuf) {}; wvar(&b, CHC_PKT_HELLO);
    if (fake_up(&f, &al, &b, REV_MODERN, &err) == CHC_OK) {
        CHECK(chc__recv_pong(&f.c, NULL, &err) == CHC_ERR_PROTOCOL);
        CHECK(strstr(err.msg, "expected Pong") != NULL);
        fake_down(&f);
    } else
        fail_count++;

    /* Exception in place of Pong, once discarded and once handed back. */
    b = (wbuf) {}; wvar(&b, CHC_PKT_EXCEPTION); wexception(&b);
    if (fake_up(&f, &al, &b, REV_MODERN, &err) == CHC_OK) {
        CHECK(chc__recv_pong(&f.c, NULL, &err) == CHC_ERR_SERVER);
        fake_down(&f);
    } else
        fail_count++;

    if (fake_up(&f, &al, &b, REV_MODERN, &err) == CHC_OK) {
        chc_exception *e = NULL;
        CHECK(chc__recv_pong(&f.c, &e, &err) == CHC_ERR_SERVER);
        CHECK(e != NULL);
        if (e) CHECK_EQ_I64(e->code, 42);
        chc_exception_free(e, &al);
        fake_down(&f);
    } else
        fail_count++;

    /* Truncated exception body propagates the read error. */
    b = (wbuf) {}; wvar(&b, CHC_PKT_EXCEPTION); w8(&b, 1);
    if (fake_up(&f, &al, &b, REV_MODERN, &err) == CHC_OK) {
        CHECK(chc__recv_pong(&f.c, NULL, &err) == CHC_ERR_EOF);
        fake_down(&f);
    } else
        fail_count++;
}

/* ---------------- send side ---------------------------------------------- */

static void
test_send_paths(void)
{
    current_test = "send_paths";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    wbuf none = {};
    fake f;

    if (fake_up(&f, &al, &none, REV_MODERN, &err) == CHC_OK) {
        CHECK(chc_client_send_ping(&f.c, &err) == CHC_OK);
        CHECK(chc_client_send_cancel(&f.c, &err) == CHC_OK);
        CHECK_EQ_U64(f.sink.len, 2);

        /* Compression on without a codec is caller error, not a crash. */
        f.c.compression = CHC_COMP_LZ4;
        f.c.codec = NULL;
        CHECK(chc_client_send_data(&f.c, NULL, &err) == CHC_ERR_USAGE);
        f.c.compression = CHC_COMP_NONE;
        fake_down(&f);
    } else
        fail_count++;

    /* Settings and parameters need server support. */
    chc_query_setting setting = { .name = "max_threads", .value = "1" };
    chc_query_param param = { .name = "p", .value = "1" };

    if (fake_up(&f, &al, &none, CHC__REV_SETTINGS_AS_STRINGS - 1, &err) == CHC_OK) {
        chc_query_opts opts = { .settings = &setting, .n_settings = 1 };
        CHECK(chc_client_send_query_ex(&f.c, "SELECT 1", 8, &opts, &err)
              == CHC_ERR_PROTOCOL);
        CHECK(strstr(err.msg, "settings unsupported") != NULL);
        fake_down(&f);
    } else
        fail_count++;

    if (fake_up(&f, &al, &none, CHC__REV_SETTINGS_AS_STRINGS - 1, &err) == CHC_OK) {
        chc_query_opts opts = { .params = &param, .n_params = 1 };
        CHECK(chc_client_send_query_ex(&f.c, "SELECT 1", 8, &opts, &err)
              == CHC_ERR_PROTOCOL);
        CHECK(strstr(err.msg, "parameters unsupported") != NULL);
        fake_down(&f);
    } else
        fail_count++;
}

/* ---------------- recv side ---------------------------------------------- */

static void
expect_packet(const char *what, const wbuf *b, uint64_t revision,
              int want_rc, chc_packet_kind want_kind)
{
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    fake f;
    if (fake_up(&f, &al, b, revision, &err) != CHC_OK) { fail_count++; return; }

    chc_packet pkt = {};
    int rc = chc_client_recv_packet(&f.c, &pkt, &err);
    if (rc != want_rc || (rc == CHC_OK && pkt.kind != want_kind)) {
        fprintf(stderr, "%s: FAIL: %s rc=%d want %d kind=%d want %d err='%s'\n",
                current_test, what, rc, want_rc, (int) pkt.kind, (int) want_kind,
                err.msg);
        fail_count++;
    }
    chc_packet_clear(&f.c, &pkt);
    fake_down(&f);
}

/* Empty block body for the revision under test. */
static void
wempty_block(wbuf *b, uint64_t revision)
{
    if (revision >= CHC__REV_BLOCK_INFO) {
        wvar(b, 1); w8(b, 0); wvar(b, 2);
        for (int i = 0; i < 4; i++) w8(b, 0);
        wvar(b, 0);
    }
    wvar(b, 0);                                 /* n_cols */
    wvar(b, 0);                                 /* n_rows */
}

static void
test_recv_packets(void)
{
    current_test = "recv_packets";
    wbuf b;

    b = (wbuf) {}; wvar(&b, 99);
    expect_packet("unknown kind", &b, REV_MODERN, CHC_ERR_PROTOCOL, CHC_PKT_HELLO);

    /* Block-bearing kinds each commit their own recv_kind. */
    static const chc_packet_kind blockish[] = {
        CHC_PKT_TOTALS, CHC_PKT_EXTREMES, CHC_PKT_LOG, CHC_PKT_PROFILE_EVENTS,
    };
    for (size_t i = 0; i < sizeof blockish / sizeof *blockish; i++) {
        b = (wbuf) {};
        wvar(&b, blockish[i]);
        wstr(&b, "");                           /* temp table name / log tag */
        wempty_block(&b, REV_MODERN);
        expect_packet("block-bearing", &b, REV_MODERN, CHC_OK, blockish[i]);
    }

    /* Pre-temporary-tables servers omit the leading string on Data. */
    b = (wbuf) {};
    wvar(&b, CHC_PKT_DATA);
    wempty_block(&b, CHC__REV_TEMPORARY_TABLES - 1);
    expect_packet("data, old revision", &b, CHC__REV_TEMPORARY_TABLES - 1,
                  CHC_OK, CHC_PKT_DATA);

    /* Truncated ProfileInfo. */
    b = (wbuf) {}; wvar(&b, CHC_PKT_PROFILE_INFO); wvar(&b, 1);
    expect_packet("short profile info", &b, REV_MODERN, CHC_ERR_EOF, CHC_PKT_HELLO);

    /* Block body that fails to parse: the partial is dropped, not returned. */
    b = (wbuf) {};
    wvar(&b, CHC_PKT_DATA);
    wstr(&b, "");
    wvar(&b, 1); w8(&b, 0); wvar(&b, 2);
    for (int i = 0; i < 4; i++) w8(&b, 0);
    wvar(&b, 0);
    wvar(&b, 1); wvar(&b, 1);                   /* one column, one row */
    wstr(&b, "c"); wstr(&b, "NoSuchType"); w8(&b, 0);
    expect_packet("bad column type", &b, REV_MODERN, CHC_ERR_TYPE, CHC_PKT_HELLO);
}

/* ---------------- compressed recv ---------------------------------------- */

static void
test_compressed_recv(void)
{
    current_test = "compressed_recv";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    wbuf b = {};
    wvar(&b, CHC_PKT_DATA);
    wstr(&b, "");
    /* Frame bytes never get read: both paths bail on the missing codec. */
    for (int i = 0; i < 32; i++) w8(&b, 0);

    fake f;
    if (fake_up(&f, &al, &b, REV_MODERN, &err) == CHC_OK) {
        f.c.compression = CHC_COMP_LZ4;
        f.c.codec = NULL;
        chc_packet pkt = {};
        CHECK(chc_client_recv_packet(&f.c, &pkt, &err) == CHC_ERR_USAGE);
        chc_packet_clear(&f.c, &pkt);
        fake_down(&f);
    } else
        fail_count++;

    /* Ioless variant routes through the resuming decompressor instead. */
    chc_client c;
    memset(&c, 0, sizeof c);
    c.al = &al;
    c.compression = CHC_COMP_LZ4;
    c.codec = NULL;
    c.client_revision = REV_MODERN;
    c.server.revision = REV_MODERN;
    if (chc_in_init_ioless(&c.in, &al) == CHC_OK) {
        CHECK(chc_in_submit(&c.in, b.d, b.n, &err) == CHC_OK);
        chc_packet pkt = {};
        CHECK(chc_client_recv_packet(&c, &pkt, &err) == CHC_ERR_USAGE);
        chc_packet_clear(&c, &pkt);
        chc__client_recv_state_free(&c);
        chc_in_free(&c.in);
    } else
        fail_count++;
}

/* Wrap a block body in a Data packet with an LZ4-framed payload. */
static size_t
compress_packet(const chc_alloc *al, const uint8_t *body, size_t body_len,
                bool corrupt, uint8_t *out, size_t out_cap)
{
    test_mem_sink packet;
    chc_io packet_io;
    chc_err err = {};
    chc_codec codec;
    chc_lz4_codec_init(&codec);

    test_mem_sink_init(&packet, &packet_io);
    if (chc__write_varuint(&packet_io, CHC_PKT_DATA, &err)
        || chc__write_string(&packet_io, "", 0, &err)
        || chc__comp_emit_chunks(&packet_io, &codec, CHC_COMP_LZ4,
                                 body, body_len, al, &err)) {
        test_mem_sink_free(&packet);
        return 0;
    }
    size_t n = packet.len <= out_cap ? packet.len : 0;
    if (n) {
        memcpy(out, packet.data, n);
        if (corrupt) out[n - 1] ^= 0xff;        /* breaks the frame hash */
    }
    test_mem_sink_free(&packet);
    return n;
}

/* A one-column block body written with the library's own encoder. */
static size_t
good_body(const chc_alloc *al, uint8_t *out, size_t out_cap)
{
    test_mem_sink body;
    chc_io body_io;
    chc_err err = {};
    chc_block_opts opts = {
        .has_block_info = true, .has_custom_serialization = true,
    };
    chc_type *t = NULL;
    if (chc_type_parse("UInt32", 6, al, &t, &err)) return 0;

    uint32_t values[2] = { 1, 2 };
    chc_column col = chc_build_fixed(values, sizeof values[0], 2);
    chc_block_col bc = { .name = "c", .name_len = 1, .type = t, .col = &col };
    test_mem_sink_init(&body, &body_io);
    int rc = chc_block_write_cols(&body_io, &bc, 1, 2, &opts, &err);
    chc_type_destroy(t, al);

    size_t n = (rc == CHC_OK && body.len <= out_cap) ? body.len : 0;
    if (n) memcpy(out, body.data, n);
    test_mem_sink_free(&body);
    return n;
}

/* Same shape, but naming a type the parser rejects. */
static size_t
bad_type_body(uint8_t *out, size_t out_cap)
{
    wbuf b = {};
    wvar(&b, 1); w8(&b, 0); wvar(&b, 2);
    for (int i = 0; i < 4; i++) w8(&b, 0);
    wvar(&b, 0);
    wvar(&b, 1); wvar(&b, 2);
    wstr(&b, "c"); wstr(&b, "NoSuchType"); w8(&b, 0);
    if (b.n > out_cap) return 0;
    memcpy(out, b.d, b.n);
    return b.n;
}

static void
test_compressed_resume(void)
{
    current_test = "compressed_resume";
    chc_alloc al = chc_alloc_stdlib();
    chc_codec codec;
    chc_lz4_codec_init(&codec);
    uint8_t body[2048], bytes[4096];

    static const struct { bool bad_type, corrupt; int want; } cases[] = {
        { false, false, CHC_OK           },
        { false, true,  CHC_ERR_PROTOCOL },   /* frame hash mismatch */
        { true,  false, CHC_ERR_TYPE     },   /* block names an unknown type */
    };

    for (size_t i = 0; i < sizeof cases / sizeof *cases; i++) {
        size_t body_len = cases[i].bad_type ? bad_type_body(body, sizeof body)
                                            : good_body(&al, body, sizeof body);
        size_t n = body_len ? compress_packet(&al, body, body_len,
                                              cases[i].corrupt, bytes, sizeof bytes)
                            : 0;
        if (!n) { fail_count++; continue; }

        chc_client c;
        memset(&c, 0, sizeof c);
        c.al = &al;
        c.compression = CHC_COMP_LZ4;
        c.codec = &codec;
        c.client_revision = REV_MODERN;
        c.server.revision = REV_MODERN;
        chc_err err = {};
        if (chc_in_init_ioless(&c.in, &al) != CHC_OK) { fail_count++; continue; }

        /* One byte at a time so the resume path takes every would-block
         * branch it has. */
        int rc = CHC_WOULD_BLOCK;
        chc_packet pkt = {};
        for (size_t fed = 0; fed <= n; fed++) {
            rc = chc_client_recv_packet(&c, &pkt, &err);
            if (rc != CHC_WOULD_BLOCK) break;
            if (fed == n) break;
            if (chc_in_submit(&c.in, bytes + fed, 1, &err) != CHC_OK) break;
        }
        if (rc != cases[i].want) {
            fprintf(stderr, "%s: FAIL: case %zu rc=%d want %d err='%s'\n",
                    current_test, i, rc, cases[i].want, err.msg);
            fail_count++;
        }
        chc_packet_clear(&c, &pkt);
        chc__client_recv_state_free(&c);
        chc_in_free(&c.in);
    }
}

/* Same stream, one failed allocation at a time: the resume path must unwind
 * its decompressor and partial block on every OOM. */
static void
test_compressed_resume_oom(void)
{
    current_test = "compressed_resume_oom";
    chc_alloc al = chc_alloc_stdlib();
    chc_codec codec;
    chc_lz4_codec_init(&codec);
    uint8_t body[2048], bytes[4096];

    size_t body_len = good_body(&al, body, sizeof body);
    size_t n = body_len ? compress_packet(&al, body, body_len, false,
                                          bytes, sizeof bytes) : 0;
    if (!n) { fail_count++; return; }

    for (size_t fail_at = 0; ; fail_at++) {
        test_fail_alloc fa;
        chc_alloc fal = test_fail_alloc_init(&fa, fail_at);
        chc_client c;
        memset(&c, 0, sizeof c);
        c.al = &fal;
        c.compression = CHC_COMP_LZ4;
        c.codec = &codec;
        c.client_revision = REV_MODERN;
        c.server.revision = REV_MODERN;
        chc_err err = {};

        int rc = chc_in_init_ioless(&c.in, &fal);
        if (rc == CHC_OK) rc = chc_in_submit(&c.in, bytes, n, &err);
        chc_packet pkt = {};
        if (rc == CHC_OK) rc = chc_client_recv_packet(&c, &pkt, &err);
        chc_packet_clear(&c, &pkt);
        chc__client_recv_state_free(&c);
        chc_in_free(&c.in);

        if (rc != CHC_OK && rc != CHC_ERR_OOM) {
            fprintf(stderr, "%s: FAIL: fail_at=%zu rc=%d err='%s'\n",
                    current_test, fail_at, rc, err.msg);
            fail_count++;
            break;
        }
        CHECK_EQ_U64(fa.live, 0);
        CHECK_EQ_U64(fa.live_bytes, 0);
        if (rc == CHC_OK && fail_at >= fa.calls) break;
    }
}

/* ---------------- async wrappers ----------------------------------------- */

static void
test_async_wrappers(void)
{
    current_test = "async_wrappers";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_client_opts opts = {
        .client_name = "test-client",
        .database    = "db",
        .user        = "u",
        .password    = "p",
    };

    chc_async_client *c = NULL;
    CHECK(chc_async_client_init(&c, &opts, &al, &err) == CHC_OK);
    if (!c) return;

    uint32_t values[2] = { 1, 2 };
    chc_type *t = NULL;
    if (chc_type_parse("UInt32", 6, &al, &t, &err) == CHC_OK) {
        chc_column col = chc_build_fixed(values, sizeof values[0], 2);
        chc_block_col storage[1];
        chc_block_builder bb;
        chc_block_builder_init(&bb, storage);
        chc_block_builder_append(&bb, "c", 1, t, &col);
        CHECK(chc_async_send_data(c, &bb, &err) == CHC_OK);
        chc_type_destroy(t, &al);
    } else
        fail_count++;
    CHECK(chc_async_send_data_end(c, &err) == CHC_OK);
    const uint8_t *pending = NULL;
    size_t pending_len = 0;
    chc_async_pending_out(c, &pending, &pending_len);
    CHECK(pending_len > 0);
    chc_async_client_free(c);

    /* Fail each allocation in turn: init must unwind to nothing. */
    for (size_t fail_at = 0; ; fail_at++) {
        test_fail_alloc fa;
        chc_alloc fal = test_fail_alloc_init(&fa, fail_at);
        chc_async_client *ac = NULL;
        chc_err oerr = {};
        int rc = chc_async_client_init(&ac, &opts, &fal, &oerr);
        if (rc == CHC_OK) {
            chc_async_client_free(ac);
            CHECK_EQ_U64(fa.live, 0);
            if (fail_at >= fa.calls) break;
            continue;
        }
        if (rc != CHC_ERR_OOM) {
            fprintf(stderr, "%s: FAIL: init fail_at=%zu rc=%d\n",
                    current_test, fail_at, rc);
            fail_count++;
            break;
        }
        CHECK_EQ_U64(fa.live, 0);
    }
}

/* A server announcing a newer revision is clamped to what the client speaks. */
static void
test_async_revision_clamp(void)
{
    current_test = "async_revision_clamp";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_async_client *c = NULL;
    chc_client_opts opts = { .client_revision = REV_MODERN };
    if (chc_async_client_init(&c, &opts, &al, &err) != CHC_OK) { fail_count++; return; }

    wbuf b = {};
    wvar(&b, CHC_PKT_HELLO);
    wstr(&b, "ClickHouse");
    wvar(&b, 99);                               /* version major */
    wvar(&b, 1);                                /* version minor */
    wvar(&b, REV_MODERN + 100);                 /* revision ahead of ours */
    wstr(&b, "UTC");
    wstr(&b, "host");
    wvar(&b, 0);                                /* version patch */
    wvar(&b, CHC_PKT_PONG);

    CHECK(chc_async_handshake(c, NULL, &err) == CHC_WOULD_BLOCK);
    CHECK(chc_async_submit(c, b.d, b.n, &err) == CHC_OK);
    CHECK(chc_async_handshake(c, NULL, &err) == CHC_OK);
    CHECK_EQ_U64(chc_async_server_info(c)->revision, REV_MODERN);

    chc_async_client_free(c);
}

int
main(void)
{
    test_handshake_rejects();
    test_send_paths();
    test_recv_packets();
    test_compressed_recv();
    test_compressed_resume();
    test_compressed_resume_oom();
    test_async_wrappers();
    test_async_revision_clamp();

    if (fail_count) {
        fprintf(stderr, "%d failure(s)\n", fail_count);
        return 1;
    }
    printf("all client_errors tests passed\n");
    return 0;
}
