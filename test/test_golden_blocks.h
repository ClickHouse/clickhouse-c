/*
 * test_golden_blocks.h -- shared multi-block fixtures for ioless tests
 */

#ifndef CLICKHOUSE_TEST_GOLDEN_BLOCKS_H
#define CLICKHOUSE_TEST_GOLDEN_BLOCKS_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "test_common.h"

#define TEST_GOLDEN_BLOCKS 3

CHC_MAYBE_UNUSED static int
test_write_uint_string_block(chc_io *io, const chc_alloc *al,
                             const chc_block_opts *opts, int n_rows,
                             chc_err *err)
{
    int rc = -1;
    chc_block_builder *bb = NULL;
    chc_type *tu = NULL, *ts = NULL;
    uint32_t *u = malloc((size_t) n_rows * sizeof *u);
    uint64_t *off = malloc((size_t) n_rows * sizeof *off);
    char *sdata = malloc((size_t) n_rows * 12);
    if (!u || !off || !sdata) goto done;

    size_t total = 0;
    for (int i = 0; i < n_rows; i++) {
        u[i] = (uint32_t) (i * 2654435761u);
        int k = snprintf(sdata + total, 12, "%d", i);
        total += (size_t) k;
        off[i] = total;
    }
    if (chc_block_builder_init(&bb, al, err)) goto done;
    if (chc_type_parse("UInt32", 6, al, &tu, err)) goto done;
    if (chc_type_parse("String", 6, al, &ts, err)) goto done;
    if (chc_block_builder_append_fixed(bb, "u", 1, tu, u, n_rows, err))
        goto done;
    if (chc_block_builder_append_string(bb, "s", 1, off, (uint8_t *) sdata,
                                        n_rows, err))
        goto done;
    rc = chc_block_write(io, bb, opts, err);
done:
    chc_block_builder_destroy(bb);
    chc_type_destroy(tu, al);
    chc_type_destroy(ts, al);
    free(u);
    free(off);
    free(sdata);
    return rc;
}

CHC_MAYBE_UNUSED static int
test_write_nullable_array_block(chc_io *io, const chc_alloc *al,
                                const chc_block_opts *opts, chc_err *err)
{
    int rc = -1;
    chc_block_builder *bb = NULL;
    chc_type *tn = NULL, *ta = NULL;
    uint8_t  nulls[4] = { 0, 1, 0, 1 };
    uint64_t noff[4] = { 2, 2, 5, 5 };
    const uint8_t nbuf[] = "abcde";
    uint64_t aoff[4] = { 3, 3, 6, 10 };
    uint32_t aval[10] = { 1,2,3, 7,8,9, 10,11,12,13 };

    if (chc_block_builder_init(&bb, al, err)) goto done;
    if (chc_type_parse("Nullable(String)", 16, al, &tn, err)) goto done;
    if (chc_type_parse("Array(UInt32)", 13, al, &ta, err)) goto done;
    if (chc_block_builder_append_nullable_string(bb, "n", 1, tn, nulls,
            noff, nbuf, 4, err))
        goto done;
    if (chc_block_builder_append_array_fixed(bb, "a", 1, ta, aoff, aval,
            4, err))
        goto done;
    rc = chc_block_write(io, bb, opts, err);
done:
    chc_block_builder_destroy(bb);
    chc_type_destroy(tn, al);
    chc_type_destroy(ta, al);
    return rc;
}

CHC_MAYBE_UNUSED static int
test_write_lc_string_block(chc_io *io, const chc_alloc *al,
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
            1, keys, doff, ddata, 3, 5, err))
        goto done;
    rc = chc_block_write(io, bb, opts, err);
done:
    chc_block_builder_destroy(bb);
    chc_type_destroy(t, al);
    return rc;
}

CHC_MAYBE_UNUSED static uint8_t *
test_build_golden_stream(const chc_alloc *al, const chc_block_opts *opts,
                         int wide_rows, size_t *out_len)
{
    test_mem_sink s;
    chc_io io;
    test_mem_sink_init(&s, &io);
    chc_err err = {};
    if (test_write_uint_string_block(&io, al, opts, wide_rows, &err) ||
        test_write_nullable_array_block(&io, al, opts, &err) ||
        test_write_lc_string_block(&io, al, opts, &err)) {
        fprintf(stderr, "build_stream: %s\n", err.msg);
        test_mem_sink_free(&s);
        return NULL;
    }
    *out_len = s.len;
    return s.data;
}

CHC_MAYBE_UNUSED static int
test_decode_blocks_io(const uint8_t *bytes, size_t len, const chc_alloc *al,
                      const chc_block_opts *opts, chc_block **out,
                      size_t n_blocks, uint64_t *consumed, chc_err *err)
{
    test_mem_src m;
    chc_io io;
    test_mem_src_init(&m, &io, bytes, len);
    chc_in in;
    if (chc_in_init(&in, &io, al, 0, err)) return -1;
    for (size_t i = 0; i < n_blocks; i++) {
        int rc = chc__block_read_in(&in, al, opts, &out[i], err);
        if (rc != CHC_OK || !out[i]) {
            chc_in_free(&in);
            return -1;
        }
    }
    *consumed = in.consumed;
    chc_in_free(&in);
    return 0;
}

CHC_MAYBE_UNUSED static void
test_free_blocks(chc_block **b, size_t n_blocks, const chc_alloc *al)
{
    for (size_t i = 0; i < n_blocks; i++) {
        chc_block_destroy(b[i], al);
        b[i] = NULL;
    }
}

#endif
