/*
 * test_async.c -- ioless async recv driver, no server. The correctness gate
 * for clickhouse-async.h.
 *
 * Mirrors test_ioless.c's golden-chunk approach but at the recv-packet layer:
 *   - Build a post-handshake server->client byte stream with the library's own
 *     writers (Data blocks + Progress + EndOfStream).
 *   - oracle:  a hand-seeded blocking chc_client over an io-backed mem source,
 *              chc_client_recv_packet in a loop.
 *   - subject: chc_async_client fed the same blob in adversarial chunkings
 *              {1,2,3,7,64,1<<20}, driving chc_async_recv_packet; on WOULD_BLOCK
 *              feed the next chunk and retry.
 * The subject packet sequence must be byte-identical to the oracle for every
 * chunking, including across mid-block-header and mid-column splits (the 1/2/3
 * byte chunkings guarantee these).
 *
 * Uncompressed-only so it links with no extra libs under the test.sh harness
 * (test.sh adds -llz4 only for ioless/client_tcp). The compressed recv path's
 * framing (baseline rewind-to-block-start, chc__recv_block_compressed) is
 * exercised by test_ioless.c's compressed golden gate at the block layer; a
 * compressed async-level test is a noted follow-up needing -llz4 wiring.
 *
 * Also drives the handshake state machine in isolation against a hand-built
 * Hello+Pong stream, chunked to DONE.
 *
 * Run under ASan to catch partial frees across the many would-block rewinds and
 * the retained-partial frees:
 *   CFLAGS='-fsanitize=address -g' LDFLAGS='-fsanitize=address' ./test.sh async
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#define CHC_NO_LZ4                  /* uncompressed-only; stay linkable as-is */
#define CHC_NO_ZSTD
#include "clickhouse.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"
#include "clickhouse-async.h"

static int        fail_count = 0;
static const char *current_test = "";

#include "test_common.h"
#include "test_block_compare.h"

/* Fixed revision: block_info + custom_serialization + temp tables all on. */
#define TEST_REVISION CHC_CLIENT_DEFAULT_REVISION

static chc_alloc test_make_alloc(void) { return chc_alloc_stdlib(); }

/* ---------------- fixture: server->client response stream --------------- */

/* One Data packet: tag varuint + temp-table name "" + block body. */
static int
write_data_packet_uint_string(chc_io *io, const chc_alloc *al,
                              const chc_block_opts *opts, int N, chc_err *err)
{
    int rc = -1;
    chc_block_builder *bb = NULL;
    chc_type *tu = NULL, *ts = NULL;
    uint32_t *u = malloc((size_t) N * sizeof *u);
    uint64_t *off = malloc((size_t) N * sizeof *off);
    char *sdata = malloc((size_t) N * 12);
    if (!u || !off || !sdata) goto done;

    size_t total = 0;
    for (int i = 0; i < N; i++) {
        u[i] = (uint32_t) (i * 2654435761u);
        int k = snprintf(sdata + total, 12, "%d", i);
        total += (size_t) k;
        off[i] = total;
    }
    if (chc_block_builder_init(&bb, al, err)) goto done;
    if (chc_type_parse("UInt32", 6, al, &tu, err)) goto done;
    if (chc_type_parse("String", 6, al, &ts, err)) goto done;
    if (chc_block_builder_append_fixed(bb, "u", 1, tu, u, N, err)) goto done;
    if (chc_block_builder_append_string(bb, "s", 1, off, (uint8_t *) sdata, N, err)) goto done;

    if ((rc = chc__write_varuint(io, CHC_PKT_DATA, err))) goto done;
    if ((rc = chc__write_string(io, "", 0, err))) goto done;     /* temp-table name */
    rc = chc_block_write(io, bb, opts, err);
done:
    chc_block_builder_destroy(bb);
    chc_type_destroy(tu, al);
    chc_type_destroy(ts, al);
    free(u); free(off); free(sdata);
    return rc;
}

/* One Data packet carrying Nullable(String) + Array(UInt32) + LowCardinality. */
static int
write_data_packet_composite(chc_io *io, const chc_alloc *al,
                            const chc_block_opts *opts, chc_err *err)
{
    int rc = -1;
    chc_block_builder *bb = NULL;
    chc_type *tn = NULL, *ta = NULL, *tlc = NULL;
    uint8_t  nulls[4]  = { 0, 1, 0, 1 };
    uint64_t noff[4]   = { 2, 2, 5, 5 };
    const uint8_t nbuf[] = "abcde";
    uint64_t aoff[4]   = { 3, 3, 6, 10 };
    uint32_t aval[10]  = { 1,2,3, 7,8,9, 10,11,12,13 };
    uint64_t doff[3]   = { 3, 8, 12 };
    const uint8_t ddata[] = "redgreenblue";
    uint8_t keys[5] = { 0, 2, 1, 0, 2 };

    if (chc_block_builder_init(&bb, al, err)) goto done;
    if (chc_type_parse("Nullable(String)", 16, al, &tn, err)) goto done;
    if (chc_type_parse("Array(UInt32)", 13, al, &ta, err)) goto done;
    if (chc_type_parse("LowCardinality(String)", 22, al, &tlc, err)) goto done;
    if (chc_block_builder_append_nullable_string(bb, "n", 1, tn,
            nulls, noff, nbuf, 4, err)) goto done;
    if (chc_block_builder_append_array_fixed(bb, "a", 1, ta, aoff, aval, 4, err))
        goto done;
    /* composite block: 5-row LC vs 4-row others would mismatch row count, so
     * keep LC in its own packet (below); here just nullable + array, 4 rows. */
    (void) doff; (void) ddata; (void) keys; (void) tlc;

    if ((rc = chc__write_varuint(io, CHC_PKT_DATA, err))) goto done;
    if ((rc = chc__write_string(io, "", 0, err))) goto done;
    rc = chc_block_write(io, bb, opts, err);
done:
    chc_block_builder_destroy(bb);
    chc_type_destroy(tn, al);
    chc_type_destroy(ta, al);
    chc_type_destroy(tlc, al);
    return rc;
}

/* One Data packet carrying a LowCardinality(String) block. */
static int
write_data_packet_lc(chc_io *io, const chc_alloc *al,
                     const chc_block_opts *opts, chc_err *err)
{
    int rc = -1;
    chc_block_builder *bb = NULL;
    chc_type *t = NULL;
    uint64_t doff[3] = { 3, 8, 12 };
    const uint8_t ddata[] = "redgreenblue";
    uint8_t keys[5] = { 0, 2, 1, 0, 2 };

    if (chc_block_builder_init(&bb, al, err)) goto done;
    if (chc_type_parse("LowCardinality(String)", 22, al, &t, err)) goto done;
    if (chc_block_builder_append_low_cardinality_string(bb, "lc", 2, t,
            1, keys, doff, ddata, 3, 5, err)) goto done;

    if ((rc = chc__write_varuint(io, CHC_PKT_DATA, err))) goto done;
    if ((rc = chc__write_string(io, "", 0, err))) goto done;
    rc = chc_block_write(io, bb, opts, err);
done:
    chc_block_builder_destroy(bb);
    chc_type_destroy(t, al);
    return rc;
}

/* Progress packet for revision >= CHC__REV_CLIENT_WRITE_INFO (the test
 * revision): tag + 5 varuints (rows, bytes, total_rows, written_rows,
 * written_bytes). */
static int
write_progress_packet(chc_io *io, chc_err *err)
{
    int rc;
    if ((rc = chc__write_varuint(io, CHC_PKT_PROGRESS, err))) return rc;
    if ((rc = chc__write_varuint(io, 1000, err))) return rc;   /* rows */
    if ((rc = chc__write_varuint(io, 8000, err))) return rc;   /* bytes */
    if ((rc = chc__write_varuint(io, 5000, err))) return rc;   /* total_rows */
    if ((rc = chc__write_varuint(io, 7, err)))    return rc;   /* written_rows */
    if ((rc = chc__write_varuint(io, 70, err)))   return rc;   /* written_bytes */
    return CHC_OK;
}

#define SEQ_LEN 5  /* data(wide) + data(composite) + data(lc) + progress + eos */

static uint8_t *
build_response_stream(const chc_alloc *al, const chc_block_opts *opts,
                      size_t *out_len)
{
    test_mem_sink s;
    chc_io io;
    test_mem_sink_init(&s, &io);
    chc_err err = {};
    if (write_data_packet_uint_string(&io, al, opts, 3000, &err) ||
        write_data_packet_composite(&io, al, opts, &err) ||
        write_data_packet_lc(&io, al, opts, &err) ||
        write_progress_packet(&io, &err) ||
        chc__write_varuint(&io, CHC_PKT_END_OF_STREAM, &err)) {
        fprintf(stderr, "build_response_stream: %s\n", err.msg);
        test_mem_sink_free(&s);
        return NULL;
    }
    *out_len = s.len;
    return s.data;
}

/* ---------------- recorded packet sequence ------------------------------- */

typedef struct {
    chc_packet_kind kind;
    chc_block      *block;          /* owned; for DATA kinds */
    uint64_t        prog[5];        /* rows, bytes, total_rows, w_rows, w_bytes */
} rec_packet;

static void
rec_free(rec_packet *r, size_t n, const chc_alloc *al)
{
    for (size_t i = 0; i < n; i++)
        if (r[i].block) chc_block_destroy(r[i].block, al);
}

/* ---------------- oracle: blocking client over io-backed source ---------- */

static int
oracle_decode(const uint8_t *bytes, size_t len, const chc_alloc *al,
              rec_packet *out, size_t *out_n, chc_err *err)
{
    /* Hand-seed a chc_client: no handshake, io-backed in over the blob. */
    test_mem_src m;
    chc_io io;
    test_mem_src_init(&m, &io, bytes, len);

    chc_client c;
    memset(&c, 0, sizeof c);
    c.al = al;
    c.io = &io;
    c.compression = CHC_COMP_NONE;
    c.codec = NULL;
    c.client_revision = TEST_REVISION;
    c.server.revision = TEST_REVISION;
    if (chc_in_init(&c.in, &io, al, 0, err)) return -1;

    size_t n = 0;
    for (;;) {
        chc_packet pkt = {};
        int rc = chc_client_recv_packet(&c, &pkt, err);
        if (rc != CHC_OK) { chc_packet_clear(&c, &pkt); chc_in_free(&c.in); return -1; }
        out[n].kind = pkt.kind;
        switch (pkt.kind) {
        case CHC_PKT_DATA: case CHC_PKT_TOTALS: case CHC_PKT_EXTREMES:
        case CHC_PKT_LOG:  case CHC_PKT_PROFILE_EVENTS:
            out[n].block = pkt.block;    /* take ownership */
            pkt.block = NULL;            /* moved into record */
            break;
        case CHC_PKT_PROGRESS:
            out[n].prog[0] = pkt.progress.rows;
            out[n].prog[1] = pkt.progress.bytes;
            out[n].prog[2] = pkt.progress.total_rows;
            out[n].prog[3] = pkt.progress.written_rows;
            out[n].prog[4] = pkt.progress.written_bytes;
            break;
        default:
            break;
        }
        chc_packet_clear(&c, &pkt);
        n++;
        if (out[n - 1].kind == CHC_PKT_END_OF_STREAM) break;
        if (n >= SEQ_LEN + 1) break;     /* guard */
    }
    *out_n = n;
    chc_in_free(&c.in);
    return 0;
}

/* ---------------- subject: async client fed in chunks -------------------- */

static int
subject_decode(const uint8_t *bytes, size_t len, size_t chunk,
               const chc_alloc *al, rec_packet *out, size_t *out_n, chc_err *err)
{
    chc_async_client *c = NULL;
    if (chc_async_client_init(&c, NULL, al, err)) return -1;
    /* Skip handshake: seed revision and pretend handshake is done. */
    c->cli.server.revision = TEST_REVISION;

    size_t fed = 0;
    size_t n = 0;
    for (;;) {
        chc_packet pkt = {};
        chc_err e = {};
        int rc = chc_async_recv_packet(c, &pkt, &e);
        if (rc == CHC_WOULD_BLOCK) {
            if (fed >= len) {
                chc__err_set(err, CHC_ERR_EOF, "feed underrun");
                chc_async_packet_clear(c, &pkt);
                chc_async_client_free(c);
                return -1;
            }
            size_t take = (len - fed) < chunk ? (len - fed) : chunk;
            if (chc_async_submit(c, bytes + fed, take, &e)) {
                *err = e; chc_async_packet_clear(c, &pkt);
                chc_async_client_free(c); return -1;
            }
            fed += take;
            continue;
        }
        if (rc != CHC_OK) {
            *err = e; chc_async_packet_clear(c, &pkt);
            chc_async_client_free(c); return -1;
        }
        out[n].kind = pkt.kind;
        switch (pkt.kind) {
        case CHC_PKT_DATA: case CHC_PKT_TOTALS: case CHC_PKT_EXTREMES:
        case CHC_PKT_LOG:  case CHC_PKT_PROFILE_EVENTS:
            out[n].block = pkt.block;    /* take ownership */
            pkt.block = NULL;            /* moved into record */
            break;
        case CHC_PKT_PROGRESS:
            out[n].prog[0] = pkt.progress.rows;
            out[n].prog[1] = pkt.progress.bytes;
            out[n].prog[2] = pkt.progress.total_rows;
            out[n].prog[3] = pkt.progress.written_rows;
            out[n].prog[4] = pkt.progress.written_bytes;
            break;
        default:
            break;
        }
        chc_async_packet_clear(c, &pkt);
        n++;
        if (out[n - 1].kind == CHC_PKT_END_OF_STREAM) break;
        if (n >= SEQ_LEN + 1) break;
    }
    *out_n = n;
    chc_async_client_free(c);
    return 0;
}

/* ---------------- tests -------------------------------------------------- */

static void
test_recv_golden_chunk(void)
{
    current_test = "recv_golden_chunk";
    chc_alloc al = test_make_alloc();
    chc_err err = {};
    chc_block_opts opts = { .has_block_info = true, .has_custom_serialization = true };

    size_t len = 0;
    uint8_t *stream = build_response_stream(&al, &opts, &len);
    CHECK(stream != NULL); if (!stream) return;

    rec_packet oracle[SEQ_LEN + 1] = {};
    size_t on = 0;
    if (oracle_decode(stream, len, &al, oracle, &on, &err) != 0) {
        fprintf(stderr, "%s: oracle decode failed: %s\n", current_test, err.msg);
        fail_count++; free(stream); return;
    }
    CHECK_EQ_U64(on, SEQ_LEN);
    CHECK(oracle[SEQ_LEN - 1].kind == CHC_PKT_END_OF_STREAM);

    static const size_t chunks[] = { 1, 2, 3, 7, 64, 1u << 20 };
    for (size_t ci = 0; ci < sizeof chunks / sizeof *chunks; ci++) {
        rec_packet subj[SEQ_LEN + 1] = {};
        size_t sn = 0;
        chc_err e = {};
        if (subject_decode(stream, len, chunks[ci], &al, subj, &sn, &e) != 0) {
            fprintf(stderr, "%s: chunk=%zu decode failed: %s\n",
                    current_test, chunks[ci], e.msg);
            fail_count++;
            rec_free(subj, sn, &al);
            continue;
        }
        if (sn != on) {
            fprintf(stderr, "%s: chunk=%zu packet count %zu != oracle %zu\n",
                    current_test, chunks[ci], sn, on);
            fail_count++;
            rec_free(subj, sn, &al);
            continue;
        }
        for (size_t i = 0; i < on; i++) {
            if (subj[i].kind != oracle[i].kind) {
                fprintf(stderr, "%s: chunk=%zu pkt %zu kind %d != %d\n",
                        current_test, chunks[ci], i,
                        (int) subj[i].kind, (int) oracle[i].kind);
                fail_count++;
                continue;
            }
            if (oracle[i].kind == CHC_PKT_PROGRESS) {
                for (int k = 0; k < 5; k++)
                    if (subj[i].prog[k] != oracle[i].prog[k]) {
                        fprintf(stderr, "%s: chunk=%zu pkt %zu prog[%d] %llu != %llu\n",
                                current_test, chunks[ci], i, k,
                                (unsigned long long) subj[i].prog[k],
                                (unsigned long long) oracle[i].prog[k]);
                        fail_count++;
                    }
            }
            if (oracle[i].block || subj[i].block) {
                if (!test_block_eq(oracle[i].block, subj[i].block)) {
                    fprintf(stderr, "%s: chunk=%zu pkt %zu block mismatch\n",
                            current_test, chunks[ci], i);
                    fail_count++;
                }
            }
        }
        rec_free(subj, sn, &al);
    }

    rec_free(oracle, on, &al);
    free(stream);
}

/* ---------------- handshake state machine in isolation ------------------- */

/* Build a server->client Hello+Pong stream the handshake driver consumes.
 * Hello layout (chc__client_recv_hello, revision >= patch): tag, name string,
 * version_major, version_minor, revision, [timezone], [display_name],
 * [version_patch]. Then Pong tag. */
static uint8_t *
build_hello_pong(const chc_alloc *al, size_t *out_len)
{
    (void) al;
    test_mem_sink s;
    chc_io io;
    test_mem_sink_init(&s, &io);
    chc_err err = {};
    uint64_t rev = TEST_REVISION;
    if (chc__write_varuint(&io, CHC_PKT_HELLO, &err) ||
        chc__write_string(&io, "ClickHouse-test", 15, &err) ||
        chc__write_varuint(&io, 24, &err) ||        /* version_major */
        chc__write_varuint(&io, 8, &err) ||         /* version_minor */
        chc__write_varuint(&io, rev, &err) ||       /* revision */
        chc__write_string(&io, "UTC", 3, &err) ||   /* timezone (>= 54058) */
        chc__write_string(&io, "test-host", 9, &err) || /* display_name (>= 54372) */
        chc__write_varuint(&io, 3, &err) ||         /* version_patch (>= 54401) */
        chc__write_varuint(&io, CHC_PKT_PONG, &err)) {
        fprintf(stderr, "build_hello_pong: %s\n", err.msg);
        test_mem_sink_free(&s);
        return NULL;
    }
    *out_len = s.len;
    return s.data;
}

static void
test_handshake_chunked(void)
{
    current_test = "handshake_chunked";
    chc_alloc al = test_make_alloc();

    size_t len = 0;
    uint8_t *stream = build_hello_pong(&al, &len);
    CHECK(stream != NULL); if (!stream) return;

    static const size_t chunks[] = { 1, 2, 3, 7, 64, 1u << 20 };
    for (size_t ci = 0; ci < sizeof chunks / sizeof *chunks; ci++) {
        chc_async_client *c = NULL;
        chc_err e = {};
        if (chc_async_client_init(&c, NULL, &al, &e)) {
            fprintf(stderr, "%s: init failed: %s\n", current_test, e.msg);
            fail_count++; continue;
        }
        size_t fed = 0;
        int done = 0;
        for (int iter = 0; iter < 100000 && !done; iter++) {
            int rc = chc_async_handshake(c, &e);
            if (rc == CHC_OK) { done = 1; break; }
            if (rc == CHC_WOULD_BLOCK) {
                if (fed >= len) {
                    fprintf(stderr, "%s: chunk=%zu feed underrun\n",
                            current_test, chunks[ci]);
                    fail_count++; break;
                }
                size_t take = (len - fed) < chunks[ci] ? (len - fed) : chunks[ci];
                if (chc_async_submit(c, stream + fed, take, &e)) {
                    fprintf(stderr, "%s: feed: %s\n", current_test, e.msg);
                    fail_count++; break;
                }
                fed += take;
                continue;
            }
            fprintf(stderr, "%s: chunk=%zu handshake rc=%d: %s\n",
                    current_test, chunks[ci], rc, e.msg);
            fail_count++; break;
        }
        if (done) {
            const chc_server_info *si = chc_async_server_info(c);
            CHECK(si != NULL);
            CHECK(strncmp(si->name, "ClickHouse-test", 15) == 0);
            CHECK(si->revision == TEST_REVISION);
            CHECK(si->version_major == 24);
        }
        /* The Hello+Pong stream is the only outbound-consuming side effect
         * worth a glance: the driver must have buffered Hello (and Ping). */
        const uint8_t *ob; size_t ol;
        chc_async_pending_out(c, &ob, &ol);
        CHECK(ol > 0);          /* Hello + addendum + Ping were buffered */
        chc_async_client_free(c);
    }

    free(stream);
}

/* ---------------- out-buffer shuttle mechanics --------------------------- */

static void
test_out_shuttle(void)
{
    current_test = "out_shuttle";
    chc_alloc al = test_make_alloc();
    chc_err err = {};
    chc_async_client *c = NULL;
    CHECK_OK(chc_async_client_init(&c, NULL, &al, &err), err);
    c->cli.server.revision = TEST_REVISION;

    /* A query buffers bytes; draining in pieces then fully resets the sink. */
    const char *sql = "SELECT 1";
    CHECK_OK(chc_async_send_query(c, sql, strlen(sql), "", 0, &err), err);
    const uint8_t *ob; size_t ol;
    chc_async_pending_out(c, &ob, &ol);
    CHECK(ol > 0);
    size_t first = ol;

    chc_async_consume_out(c, 1);
    chc_async_pending_out(c, &ob, &ol);
    CHECK_EQ_U64(ol, first - 1);

    /* Over-consume guard: asking for more than available clamps. */
    chc_async_consume_out(c, first);            /* clamps to remaining */
    chc_async_pending_out(c, &ob, &ol);
    CHECK_EQ_U64(ol, 0);

    /* After full drain, a new send starts the sink at offset 0 again. */
    CHECK_OK(chc_async_send_query(c, sql, strlen(sql), "", 0, &err), err);
    chc_async_pending_out(c, &ob, &ol);
    CHECK_EQ_U64(ol, first);                     /* identical query, same length */
out:
    chc_async_client_free(c);
}

int
main(void)
{
    test_recv_golden_chunk();
    test_handshake_chunked();
    test_out_shuttle();

    if (fail_count) {
        fprintf(stderr, "%d check(s) failed\n", fail_count);
        return 1;
    }
    printf("all async tests passed\n");
    return 0;
}
