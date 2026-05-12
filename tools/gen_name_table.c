/* gen_name_table.c -- offline generator for chc__name_to_kind's perfect
 * hash table. Compiled & run by tools/regen_name_table.sh, never linked
 * into the library.
 *
 * Output: a C array initializer plus a CHC__NAME_TABLE_M #define,
 * intended to be spliced into clickhouse.h between the
 *   AUTO-GENERATED-NAME-TABLE-{BEGIN,END}
 * sentinel comments.
 *
 * Hash function and key formula match the runtime in clickhouse.h:
 *   h = chc__city_hash_len_0_to_16(s, min(n, 16)) ^ (uint64_t) n;
 *   bucket = h & (M - 1);
 *
 * Strategy: try M = 64, 128, 256, ... until no two entries collide
 * on the same bucket. Emit the smallest M that works.
 */

#define CHC_IMPLEMENTATION
#include "../clickhouse.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *name;
    const char *kind;       /* enum spelling, e.g. "CHC_INT8" */
} row;

static const row rows[] = {
    {"Int8",     "CHC_INT8"},     {"Int16",     "CHC_INT16"},
    {"Int32",    "CHC_INT32"},    {"Int64",     "CHC_INT64"},
    {"Int128",   "CHC_INT128"},   {"Int256",    "CHC_INT256"},
    {"UInt8",    "CHC_UINT8"},    {"UInt16",    "CHC_UINT16"},
    {"UInt32",   "CHC_UINT32"},   {"UInt64",    "CHC_UINT64"},
    {"UInt128",  "CHC_UINT128"},  {"UInt256",   "CHC_UINT256"},
    {"Float32",  "CHC_FLOAT32"},  {"Float64",   "CHC_FLOAT64"},
    {"BFloat16", "CHC_BFLOAT16"}, {"Bool",      "CHC_BOOL"},
    {"String",   "CHC_STRING"},   {"FixedString", "CHC_FIXED_STRING"},
    {"Date",     "CHC_DATE"},     {"Date32",    "CHC_DATE32"},
    {"DateTime", "CHC_DATETIME"}, {"DateTime64","CHC_DATETIME64"},
    {"Time",     "CHC_TIME"},     {"Time64",    "CHC_TIME64"},
    {"UUID",     "CHC_UUID"},     {"IPv4",      "CHC_IPV4"},
    {"IPv6",     "CHC_IPV6"},
    {"Enum8",    "CHC_ENUM8"},    {"Enum16",    "CHC_ENUM16"},
    {"Decimal32",  "CHC_DECIMAL32"},  {"Decimal64",  "CHC_DECIMAL64"},
    {"Decimal128", "CHC_DECIMAL128"}, {"Decimal256", "CHC_DECIMAL256"},
    /* "Decimal" stays out -- handled inline by parser (decimal_alias). */
    {"Nullable", "CHC_NULLABLE"}, {"Array",     "CHC_ARRAY"},
    {"Tuple",    "CHC_TUPLE"},    {"Map",       "CHC_MAP"},
    {"Nested",   "CHC_NESTED"},
    {"LowCardinality", "CHC_LOW_CARDINALITY"},
    {"Nothing",  "CHC_NOTHING"},  {"Void",      "CHC_VOID"},
    {"Point",    "CHC_POINT"},    {"Ring",      "CHC_RING"},
    {"Polygon",  "CHC_POLYGON"},  {"MultiPolygon", "CHC_MULTI_POLYGON"},
    {"SimpleAggregateFunction", "CHC_SIMPLE_AGGREGATE_FUNCTION"},
    {"AggregateFunction",       "CHC_AGGREGATE_FUNCTION"},
    {"Variant",  "CHC_VARIANT"},  {"Dynamic",   "CHC_DYNAMIC"},
    {"JSON",     "CHC_JSON"},     {"Object",    "CHC_OBJECT"},
    {"IntervalNanosecond",  "CHC_INTERVAL"},
    {"IntervalMicrosecond", "CHC_INTERVAL"},
    {"IntervalMillisecond", "CHC_INTERVAL"},
    {"IntervalSecond",      "CHC_INTERVAL"},
    {"IntervalMinute",      "CHC_INTERVAL"},
    {"IntervalHour",        "CHC_INTERVAL"},
    {"IntervalDay",         "CHC_INTERVAL"},
    {"IntervalWeek",        "CHC_INTERVAL"},
    {"IntervalMonth",       "CHC_INTERVAL"},
    {"IntervalQuarter",     "CHC_INTERVAL"},
    {"IntervalYear",        "CHC_INTERVAL"},
};

static uint64_t key_of(const char *s, size_t n, uint64_t seed)
{
    size_t h_len = n < 16 ? n : 16;
    return chc__city_hash_len_16(
        chc__city_hash_len_0_to_16(s, h_len) + (uint64_t) n, seed);
}

static int try_M_seed(size_t M, uint64_t seed, size_t n_rows)
{
    unsigned char *seen = calloc(M, 1);
    int ok = 1;
    for (size_t i = 0; i < n_rows; i++) {
        size_t nlen = strlen(rows[i].name);
        uint64_t h = key_of(rows[i].name, nlen, seed);
        size_t b = (size_t)(h & (M - 1));
        if (seen[b]) { ok = 0; break; }
        seen[b] = 1;
    }
    free(seen);
    return ok;
}

int main(void)
{
    size_t n_rows = sizeof rows / sizeof rows[0];

    size_t M = 0;
    uint64_t seed = 0;
    for (size_t cand = 64; cand <= 4096; cand <<= 1) {
        int found = 0;
        for (uint64_t s = 0; s < 200000; s++) {
            if (try_M_seed(cand, s, n_rows)) { seed = s; found = 1; break; }
        }
        if (found) { M = cand; break; }
    }

    if (!M) {
        fprintf(stderr, "gen_name_table: no collision-free (M,seed) found\n");
        return 1;
    }

    fprintf(stderr, "gen_name_table: %zu entries, M=%zu, seed=%llu\n",
            n_rows, M, (unsigned long long) seed);

    printf("#define CHC__NAME_TABLE_M %zuu\n", M);
    printf("#define CHC__NAME_TABLE_SEED %lluull\n", (unsigned long long) seed);
    printf("struct chc__name_row { const char *name; chc_kind kind; };\n");
    printf("static const struct chc__name_row chc__name_table[CHC__NAME_TABLE_M] = {\n");

    int *slot = malloc(M * sizeof *slot);
    for (size_t i = 0; i < M; i++) slot[i] = -1;
    for (size_t i = 0; i < n_rows; i++) {
        size_t nlen = strlen(rows[i].name);
        uint64_t h = key_of(rows[i].name, nlen, seed);
        slot[h & (M - 1)] = (int) i;
    }
    for (size_t b = 0; b < M; b++) {
        int i = slot[b];
        if (i < 0) {
            printf("    [%3zu] = {0},\n", b);
        } else {
            printf("    [%3zu] = {\"%s\", %s},\n",
                   b, rows[i].name, rows[i].kind);
        }
    }
    printf("};\n");
    free(slot);
    return 0;
}
