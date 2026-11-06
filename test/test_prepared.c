/*
 * test_prepared.c -- prepared Data blocks, no server. A prepared block sent on
 * a connection writes the same bytes chc_client_send_data writes for that
 * block, in every compression mode, and a connection negotiated differently
 * refuses it without writing.
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"
#include "clickhouse-async.h"

static int        fail_count = 0;
static const char *current_test = "";

#include "test_common.h"

#define N_ROWS 40000   /* > CHC_COMPRESS_MAX_CHUNK serialized: several frames */

typedef struct {
    uint64_t   ids[N_ROWS];
    uint64_t   offs[N_ROWS];
    uint8_t    text[N_ROWS * 3];
    chc_type  *t_id, *t_text;
    chc_column c_id, c_text;
    chc_block_col cols[2];
    chc_block_builder bb;
} fixture;

static int
fixture_init(fixture *f, const chc_alloc *al, chc_err *err)
{
    size_t at = 0;
    for (size_t i = 0; i < N_ROWS; i++) {
        f->ids[i] = i * 2654435761u;
        size_t n = i % 4;
        for (size_t k = 0; k < n; k++) f->text[at++] = (uint8_t) ('a' + k);
        f->offs[i] = at;
    }
    int rc = chc_type_parse("UInt64", 6, al, &f->t_id, err);
    if (rc != CHC_OK) return rc;
    rc = chc_type_parse("String", 6, al, &f->t_text, err);
    if (rc != CHC_OK) return rc;
    f->c_id = chc_build_fixed(f->ids, 8, N_ROWS);
    f->c_text = chc_build_string(f->offs, f->text, N_ROWS);
    chc_block_builder_init(&f->bb, f->cols);
    chc_block_builder_append(&f->bb, "id", 2, f->t_id, &f->c_id);
    chc_block_builder_append(&f->bb, "text", 4, f->t_text, &f->c_text);
    return CHC_OK;
}

static void
fixture_free(fixture *f, const chc_alloc *al)
{
    chc_type_destroy(f->t_id, al);
    chc_type_destroy(f->t_text, al);
}

/* Client writing into `sink`, as if negotiated at `revision` */
static void
client_over(chc_client *c, chc_io *io, const chc_alloc *al, chc_compression m,
            const chc_codec *codec, uint64_t revision)
{
    memset(c, 0, sizeof *c);
    c->io = io;
    c->al = al;
    c->compression = m;
    c->codec = m == CHC_COMP_NONE ? NULL : codec;
    c->server.revision = revision;
}

static void
test_matches_send_data(void)
{
    current_test = "matches_send_data";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    static fixture f;
    if (fixture_init(&f, &al, &err) != CHC_OK) { fail_count++; return; }
    chc_codec codec = {};
    chc_lz4_codec_init(&codec);
    chc_zstd_codec_init(&codec);
    chc_compression modes[] = { CHC_COMP_NONE, CHC_COMP_LZ4, CHC_COMP_ZSTD };
    for (size_t m = 0; m < 3; m++) {
        test_mem_sink direct, prepared;
        chc_io direct_io, prepared_io;
        test_mem_sink_init(&direct, &direct_io);
        test_mem_sink_init(&prepared, &prepared_io);
        chc_client c;
        chc_prepared_block pb = {};

        client_over(&c, &direct_io, &al, modes[m], &codec, CHC_CLIENT_REVISION);
        CHECK_OK(chc_client_send_data(&c, &f.bb, &err), err);

        CHECK_OK(chc_block_prepare(&f.bb, modes[m], &codec, &al, &pb, &err), err);
        CHECK_EQ_U64(pb.n_rows, N_ROWS);
        CHECK(pb.raw_len > CHC_COMPRESS_MAX_CHUNK);
        if (modes[m] == CHC_COMP_NONE)
            CHECK_EQ_U64(pb.len, pb.raw_len);
        else
            CHECK(pb.len < pb.raw_len);
        /* Reuse bytes across supported revisions. */
        for (int i = 0; i < 2; i++) {
            uint64_t revision = i ? CHC_CLIENT_REVISION : CHC_SERVER_MIN_REVISION;
            client_over(&c, &prepared_io, &al, modes[m], &codec, revision);
            size_t before = prepared.len;
            CHECK_OK(chc_client_send_prepared(&c, &pb, &err), err);
            CHECK_EQ_U64(prepared.len - before, direct.len);
            CHECK(memcmp(prepared.data + before, direct.data, direct.len) == 0);
        }

        /* Reject mismatched compression before writing. */
        size_t len = prepared.len;
        client_over(&c, &prepared_io, &al, modes[(m + 1) % 3], &codec, CHC_CLIENT_REVISION);
        CHECK(chc_client_send_prepared(&c, &pb, &err) == CHC_ERR_USAGE);
        CHECK_EQ_U64(prepared.len, len);
    out:
        chc_prepared_block_free(&pb, &al);
        CHECK(pb.bytes == NULL);
        test_mem_sink_free(&direct);
        test_mem_sink_free(&prepared);
    }
    fixture_free(&f, &al);
}

static void
test_empty_and_errors(void)
{
    current_test = "empty_and_errors";
    chc_alloc al = chc_alloc_stdlib();
    chc_err err = {};
    chc_prepared_block pb = {};
    test_mem_sink direct, prepared;
    chc_io direct_io, prepared_io;
    test_mem_sink_init(&direct, &direct_io);
    test_mem_sink_init(&prepared, &prepared_io);
    chc_client c;

    /* NULL builder prepares the end-of-INSERT block */
    client_over(&c, &direct_io, &al, CHC_COMP_NONE, NULL, CHC_CLIENT_REVISION);
    CHECK_OK(chc_client_send_data(&c, NULL, &err), err);
    CHECK_OK(chc_block_prepare(NULL, CHC_COMP_NONE, NULL, &al, &pb, &err), err);
    client_over(&c, &prepared_io, &al, CHC_COMP_NONE, NULL, CHC_CLIENT_REVISION);
    CHECK_OK(chc_client_send_prepared(&c, &pb, &err), err);
    CHECK_EQ_U64(prepared.len, direct.len);
    CHECK(memcmp(prepared.data, direct.data, direct.len) == 0);
    chc_prepared_block_free(&pb, &al);

    /* Compression needs a codec */
    CHECK(chc_block_prepare(NULL, CHC_COMP_LZ4, NULL, &al, &pb, &err) == CHC_ERR_USAGE);
    CHECK(pb.bytes == NULL);

    /* Async wrapper forwards */
    {
        chc_async_client ac;
        memset(&ac, 0, sizeof ac);
        client_over(&ac.cli, &prepared_io, &al, CHC_COMP_ZSTD, NULL, CHC_CLIENT_REVISION);
        CHECK_OK(chc_block_prepare(NULL, CHC_COMP_NONE, NULL, &al, &pb, &err), err);
        CHECK(chc_async_send_prepared(&ac, &pb, &err) == CHC_ERR_USAGE);
        ac.cli.compression = CHC_COMP_NONE;
        CHECK_OK(chc_async_send_prepared(&ac, &pb, &err), err);
    }
out:
    chc_prepared_block_free(&pb, &al);
    chc_prepared_block_free(NULL, &al);
    test_mem_sink_free(&direct);
    test_mem_sink_free(&prepared);
}

int
main(void)
{
    test_matches_send_data();
    test_empty_and_errors();

    if (fail_count) {
        fprintf(stderr, "%d failure(s)\n", fail_count);
        return 1;
    }
    printf("all prepared tests passed\n");
    return 0;
}
