/*
 * test_common.h -- shared CHECK macros for the test binaries.
 *
 * Each test .c keeps its own file-local storage:
 *   static int        fail_count   = 0;
 *   static const char *current_test = "";
 *
 * CHECK_OK jumps to a label named `out` in the caller.
 */

#ifndef CLICKHOUSE_TEST_COMMON_H
#define CLICKHOUSE_TEST_COMMON_H

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(cond) do {                                            \
    if (!(cond)) {                                                  \
        fprintf(stderr, "%s:%d %s: FAIL: %s\n",                     \
                __FILE__, __LINE__, current_test, #cond);           \
        fail_count++;                                               \
    }                                                               \
} while (0)

#define CHECK_EQ_U64(actual, expected) do {                         \
    uint64_t a = (uint64_t) (actual), b = (uint64_t) (expected);    \
    if (a != b) {                                                   \
        fprintf(stderr, "%s:%d %s: FAIL: %s == %" PRIu64            \
                " (got %" PRIu64 ")\n",                             \
                __FILE__, __LINE__, current_test, #actual, b, a);   \
        fail_count++;                                               \
    }                                                               \
} while (0)

#define CHECK_EQ_I64(actual, expected) do {                         \
    int64_t a = (int64_t) (actual), b = (int64_t) (expected);       \
    if (a != b) {                                                   \
        fprintf(stderr, "%s:%d %s: FAIL: %s == %" PRId64            \
                " (got %" PRId64 ")\n",                             \
                __FILE__, __LINE__, current_test, #actual, b, a);   \
        fail_count++;                                               \
    }                                                               \
} while (0)

#define CHECK_STR_EQ(actual, alen, expected) do {                   \
    size_t elen = strlen(expected);                                 \
    if ((alen) != elen || memcmp((actual), (expected), elen) != 0) {\
        fprintf(stderr, "%s:%d %s: FAIL: \"%.*s\" != \"%s\"\n",     \
                __FILE__, __LINE__, current_test,                   \
                (int)(alen), (actual), (expected));                 \
        fail_count++;                                               \
    }                                                               \
} while (0)

/* CHECK_OK: jumps to `out:` on failure. Pair with chc_err err = {0};
 * and an out: label that frees / closes / returns. */
#define CHECK_OK(rc, err) do {                                      \
    if ((rc) != CHC_OK) {                                           \
        fprintf(stderr, "%s:%d %s: FAIL: rc=%d err='%s'\n",         \
                __FILE__, __LINE__, current_test, (rc), (err).msg); \
        fail_count++; goto out;                                     \
    }                                                               \
} while (0)

typedef struct test_mem_src {
    const uint8_t *data;
    size_t len;
    size_t pos;
} test_mem_src;

typedef struct test_mem_sink {
    uint8_t *data;
    size_t len;
    size_t cap;
} test_mem_sink;

CHC_MAYBE_UNUSED static int
test_mem_err_set(chc_err *err, int code, const char *msg)
{
    if (err) {
        err->code = code;
        err->server_code = 0;
        snprintf(err->msg, sizeof err->msg, "%s", msg);
        err->server_name[0] = '\0';
    }
    return code;
}

CHC_MAYBE_UNUSED static int
test_mem_read(void *ud, void *buf, size_t len, size_t *out_n, chc_err *err)
{
    (void) err;
    test_mem_src *m = ud;
    size_t avail = m->len - m->pos;
    size_t take = len < avail ? len : avail;
    if (take) memcpy(buf, m->data + m->pos, take);
    m->pos += take;
    *out_n = take;
    return CHC_OK;
}

CHC_MAYBE_UNUSED static void
test_mem_src_init(test_mem_src *src, chc_io *io, const void *data, size_t len)
{
    src->data = (const uint8_t *) data;
    src->len = len;
    src->pos = 0;
    io->ud = src;
    io->read = test_mem_read;
    io->write = NULL;
    io->check_cancel = NULL;
}

CHC_MAYBE_UNUSED static int
test_mem_sink_write(void *ud, const void *buf, size_t n, chc_err *err)
{
    test_mem_sink *s = ud;
    if (n > SIZE_MAX - s->len)
        return test_mem_err_set(err, CHC_ERR_OOM, "mem sink size overflow");

    size_t need = s->len + n;
    if (need > s->cap) {
        size_t nc = s->cap ? s->cap : 256;
        while (nc < need) {
            if (nc > SIZE_MAX / 2) { nc = need; break; }
            nc *= 2;
        }
        uint8_t *nb = realloc(s->data, nc);
        if (!nb) return test_mem_err_set(err, CHC_ERR_OOM, "mem sink oom");
        s->data = nb;
        s->cap = nc;
    }
    if (n) memcpy(s->data + s->len, buf, n);
    s->len += n;
    return CHC_OK;
}

CHC_MAYBE_UNUSED static void
test_mem_sink_init(test_mem_sink *sink, chc_io *io)
{
    memset(sink, 0, sizeof *sink);
    io->ud = sink;
    io->read = NULL;
    io->write = test_mem_sink_write;
    io->check_cancel = NULL;
}

CHC_MAYBE_UNUSED static void
test_mem_sink_free(test_mem_sink *sink)
{
    free(sink->data);
    sink->data = NULL;
    sink->len = 0;
    sink->cap = 0;
}

#endif
