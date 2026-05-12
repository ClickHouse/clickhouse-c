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

#endif
