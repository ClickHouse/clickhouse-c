/* bench_name_lookup.c -- microbench for chc__name_to_kind.
 *
 *   cc -std=c11 -O2 -I.. examples/bench_name_lookup.c -o /tmp/chc_bench_name
 *   /tmp/chc_bench_name
 *
 * Loops a weighted mix of type names plus a few unknowns through the
 * (static) name->kind lookup and reports ns/lookup. Use to compare the
 * linear-scan original against the hash-table version.
 *
 * The function we're benching is static, so we include the header.
 * Make it visible by forcing the impl into this TU.
 */

#define _POSIX_C_SOURCE 200809L

#define CHC_IMPLEMENTATION
#include "../clickhouse.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Pre-optimization baseline, copied verbatim so we can measure speedup
 * without checking out the old code. Drop this once the comparison has
 * served its purpose. */
static chc_kind
linear_name_to_kind(const char *s, size_t n)
{
    struct { const char *name; chc_kind k; } map[] = {
        {"Int8",CHC_INT8},{"Int16",CHC_INT16},{"Int32",CHC_INT32},
        {"Int64",CHC_INT64},{"Int128",CHC_INT128},{"Int256",CHC_INT256},
        {"UInt8",CHC_UINT8},{"UInt16",CHC_UINT16},{"UInt32",CHC_UINT32},
        {"UInt64",CHC_UINT64},{"UInt128",CHC_UINT128},{"UInt256",CHC_UINT256},
        {"Float32",CHC_FLOAT32},{"Float64",CHC_FLOAT64},
        {"BFloat16",CHC_BFLOAT16},{"Bool",CHC_BOOL},
        {"String",CHC_STRING},{"FixedString",CHC_FIXED_STRING},
        {"Date",CHC_DATE},{"Date32",CHC_DATE32},
        {"DateTime",CHC_DATETIME},{"DateTime64",CHC_DATETIME64},
        {"Time",CHC_TIME},{"Time64",CHC_TIME64},
        {"UUID",CHC_UUID},{"IPv4",CHC_IPV4},{"IPv6",CHC_IPV6},
        {"Enum8",CHC_ENUM8},{"Enum16",CHC_ENUM16},
        {"Decimal32",CHC_DECIMAL32},{"Decimal64",CHC_DECIMAL64},
        {"Decimal128",CHC_DECIMAL128},{"Decimal256",CHC_DECIMAL256},
        {"Nullable",CHC_NULLABLE},{"Array",CHC_ARRAY},
        {"Tuple",CHC_TUPLE},{"Map",CHC_MAP},
        {"Nested",CHC_NESTED},{"LowCardinality",CHC_LOW_CARDINALITY},
        {"Nothing",CHC_NOTHING},{"Void",CHC_VOID},
        {"Point",CHC_POINT},{"Ring",CHC_RING},
        {"Polygon",CHC_POLYGON},{"MultiPolygon",CHC_MULTI_POLYGON},
        {"SimpleAggregateFunction",CHC_SIMPLE_AGGREGATE_FUNCTION},
        {"AggregateFunction",CHC_AGGREGATE_FUNCTION},
        {"Variant",CHC_VARIANT},{"Dynamic",CHC_DYNAMIC},
        {"JSON",CHC_JSON},{"Object",CHC_OBJECT},
        {"IntervalNanosecond",CHC_INTERVAL},{"IntervalMicrosecond",CHC_INTERVAL},
        {"IntervalMillisecond",CHC_INTERVAL},{"IntervalSecond",CHC_INTERVAL},
        {"IntervalMinute",CHC_INTERVAL},{"IntervalHour",CHC_INTERVAL},
        {"IntervalDay",CHC_INTERVAL},{"IntervalWeek",CHC_INTERVAL},
        {"IntervalMonth",CHC_INTERVAL},{"IntervalQuarter",CHC_INTERVAL},
        {"IntervalYear",CHC_INTERVAL},
    };
    for (size_t i = 0; i < sizeof map / sizeof map[0]; i++) {
        if (strlen(map[i].name) == n && memcmp(map[i].name, s, n) == 0)
            return map[i].k;
    }
    return CHC_VOID;
}

struct sample { const char *name; size_t len; };

static struct sample samples[] = {
#define S(s) { s, sizeof (s) - 1 }
    /* Weights echo what a typical OLAP schema looks like. */
    S("Int32"), S("Int32"), S("Int32"),
    S("UInt64"), S("UInt64"), S("UInt64"),
    S("String"), S("String"), S("String"), S("String"),
    S("Nullable"), S("Nullable"), S("Nullable"),
    S("Array"), S("Array"),
    S("LowCardinality"),
    S("DateTime"), S("DateTime64"),
    S("Float64"),
    S("Tuple"), S("Map"),
    S("FixedString"),
    S("UUID"), S("IPv4"), S("IPv6"),
    S("Decimal32"), S("Decimal64"), S("Decimal128"),
    S("IntervalSecond"), S("IntervalMinute"), S("IntervalDay"),
    S("AggregateFunction"), S("SimpleAggregateFunction"),
    S("Variant"), S("Dynamic"), S("JSON"),
    /* Miss-path: names that aren't in the table. */
    S("NotAType"), S("Garbage"), S("Decimal"),
#undef S
};

int main(void)
{
    const size_t n_samples = sizeof samples / sizeof samples[0];
    const size_t iters = 50000000;

    volatile int sink = 0;
    for (size_t i = 0; i < n_samples; i++) {
        sink ^= chc__name_to_kind(samples[i].name, samples[i].len);
        sink ^= linear_name_to_kind(samples[i].name, samples[i].len);
    }

    struct timespec t0, t1;
    double hash_ns, linear_ns;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (size_t i = 0; i < iters; i++) {
        const struct sample *s = &samples[i % n_samples];
        sink ^= chc__name_to_kind(s->name, s->len);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    hash_ns = (t1.tv_sec - t0.tv_sec) * 1e9 + (t1.tv_nsec - t0.tv_nsec);

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (size_t i = 0; i < iters; i++) {
        const struct sample *s = &samples[i % n_samples];
        sink ^= linear_name_to_kind(s->name, s->len);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    linear_ns = (t1.tv_sec - t0.tv_sec) * 1e9 + (t1.tv_nsec - t0.tv_nsec);

    printf("iterations:  %zu\n", iters);
    printf("hash:        %.2f ns/lookup  (%.3f ms total)\n",
           hash_ns / iters, hash_ns / 1e6);
    printf("linear:      %.2f ns/lookup  (%.3f ms total)\n",
           linear_ns / iters, linear_ns / 1e6);
    printf("speedup:     %.2fx\n", linear_ns / hash_ns);
    printf("sink: %d (ignore)\n", sink);
    return 0;
}
