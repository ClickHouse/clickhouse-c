/*
 * clickhouse.h -- core C client for ClickHouse Native wire format.
 *
 * Single-header library, stb style: exactly one translation unit in
 * the consumer's build must `#define CHC_IMPLEMENTATION` before including
 * this header. Other TUs include for declarations only.
 *
 * Scope here: type-name parser + AST, varint codec, block reader,
 * column accessors, block writer. No I/O backend, no TCP loop, no
 * compression -- those live in sibling headers (clickhouse-posix-io.h,
 * clickhouse-client.h, clickhouse-compression.h, ...).
 *
 * License: Apache-2.0. See LICENSE.
 */

#ifndef CLICKHOUSE_H
#define CLICKHOUSE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* -------------------------------------------------------------------------- */
/* C23 portability shim                                                       */
/* -------------------------------------------------------------------------- */
/* Macros use C23 features when available, degrading to GNU/builtin equivalent,
 * then to harmless nop, so consumers may build under any -std >= c11 */

#if defined(__has_c_attribute)
#  define CHC__HAS_ATTR(x) __has_c_attribute(x)
#else
#  define CHC__HAS_ATTR(x) 0
#endif

#if CHC__HAS_ATTR(nodiscard)
#  define CHC_NODISCARD [[nodiscard]]
#elif defined(__GNUC__) || defined(__clang__)
#  define CHC_NODISCARD __attribute__((warn_unused_result))
#else
#  define CHC_NODISCARD
#endif

#if CHC__HAS_ATTR(maybe_unused)
#  define CHC_MAYBE_UNUSED [[maybe_unused]]
#elif defined(__GNUC__) || defined(__clang__)
#  define CHC_MAYBE_UNUSED __attribute__((unused))
#else
#  define CHC_MAYBE_UNUSED
#endif

#if CHC__HAS_ATTR(deprecated)
#  define CHC_DEPRECATED(msg) [[deprecated(msg)]]
#elif defined(__GNUC__) || defined(__clang__)
#  define CHC_DEPRECATED(msg) __attribute__((deprecated(msg)))
#else
#  define CHC_DEPRECATED(msg)
#endif

#if CHC__HAS_ATTR(unsequenced)
#  define CHC_UNSEQUENCED [[unsequenced]]
#else
#  define CHC_UNSEQUENCED
#endif

#if CHC__HAS_ATTR(reproducible)
#  define CHC_REPRODUCIBLE [[reproducible]]
#else
#  define CHC_REPRODUCIBLE
#endif

/* ckd_mul (C23 <stdckdint.h>) backs chc__mul_size; see CHC__HAVE_CKD_MUL. */
#if defined(__has_include)
#  if __has_include(<stdckdint.h>)
#    include <stdckdint.h>
#    define CHC__HAVE_CKD_MUL 1
#  endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------- */
/* Errors                                                                     */
/* -------------------------------------------------------------------------- */

enum {
    CHC_OK,
    CHC_ERR_IO,
    CHC_ERR_EOF,
    CHC_ERR_PROTOCOL,
    CHC_ERR_TYPE,
    CHC_ERR_OOM,
    CHC_ERR_CANCELLED,
    CHC_ERR_SERVER,
    CHC_ERR_USAGE,
    CHC_WOULD_BLOCK
};

#ifndef CHC_ERR_MSG_LEN
#define CHC_ERR_MSG_LEN 256
#endif

typedef struct chc_err {
    int  code;
    int  server_code;
    char msg[CHC_ERR_MSG_LEN];
    char server_name[64];
} chc_err;

static inline void chc_err_reset(chc_err *e) {
    if (!e) return;
    e->code = 0;
    e->server_code = 0;
    e->msg[0] = '\0';
    e->server_name[0] = '\0';
}

/* -------------------------------------------------------------------------- */
/* Allocator                                                                  */
/* -------------------------------------------------------------------------- */

typedef struct chc_alloc {
    void *ud;
    void *(*alloc)(void *ud, size_t bytes);
    void *(*realloc)(void *ud, void *p, size_t old_bytes, size_t new_bytes);
    void  (*free)(void *ud, void *p, size_t bytes);
} chc_alloc;

#ifdef CHC_PROVIDE_STDLIB_ALLOC
chc_alloc chc_alloc_stdlib(void);
#endif

/* -------------------------------------------------------------------------- */
/* I/O                                                                        */
/* -------------------------------------------------------------------------- */

typedef struct chc_io {
    void *ud;
    int (*read)(void *ud, void *buf, size_t len, size_t *out_n, chc_err *err);
    int (*write)(void *ud, const void *buf, size_t len, chc_err *err);
    int (*check_cancel)(void *ud);
} chc_io;

/* -------------------------------------------------------------------------- */
/* Buffered reader                                                            */
/* -------------------------------------------------------------------------- */

/* Buffered input parser pulls from. Two modes:
 *  - io-backed (chc_in_init): refills from a chc_io, for blocking io.
 *  - ioless (chc_in_init_ioless): bytes received via chc_in_submit;
 *    reading past submitted bytes returns CHC_WOULD_BLOCK, leaving
 *    in-progress parse to be retried once more bytes arrive. */
typedef struct chc_in chc_in;

CHC_NODISCARD int    chc_in_init(chc_in *in, chc_io *io, const chc_alloc *al,
                                 size_t cap, chc_err *err);
CHC_NODISCARD int    chc_in_init_ioless(chc_in *in, const chc_alloc *al);
CHC_NODISCARD int    chc_in_submit(chc_in *in, const void *buf, size_t len,
                                 chc_err *err);
size_t               chc_in_available(const chc_in *in); /* unconsumed bytes */
void                 chc_in_reset(chc_in *in);   /* drop consumed, compact */
void                 chc_in_free(chc_in *in);

/* -------------------------------------------------------------------------- */
/* Type AST                                                                   */
/* -------------------------------------------------------------------------- */

typedef enum chc_kind {
    CHC_VOID = 0,
    CHC_INT8, CHC_INT16, CHC_INT32, CHC_INT64, CHC_INT128, CHC_INT256,
    CHC_UINT8, CHC_UINT16, CHC_UINT32, CHC_UINT64, CHC_UINT128, CHC_UINT256,
    CHC_FLOAT32, CHC_FLOAT64, CHC_BFLOAT16,
    CHC_BOOL,
    CHC_DATE, CHC_DATE32,
    CHC_DATETIME, CHC_DATETIME64,
    CHC_TIME, CHC_TIME64,
    CHC_STRING, CHC_FIXED_STRING,
    CHC_DECIMAL32, CHC_DECIMAL64, CHC_DECIMAL128, CHC_DECIMAL256,
    CHC_UUID, CHC_IPV4, CHC_IPV6,
    CHC_ENUM8, CHC_ENUM16,
    CHC_NULLABLE, CHC_ARRAY, CHC_TUPLE, CHC_MAP, CHC_NESTED,
    CHC_LOW_CARDINALITY,
    CHC_INTERVAL,
    CHC_POINT, CHC_RING, CHC_POLYGON, CHC_MULTI_POLYGON,
    CHC_VARIANT, CHC_DYNAMIC, CHC_JSON, CHC_OBJECT,
    CHC_AGGREGATE_FUNCTION, CHC_SIMPLE_AGGREGATE_FUNCTION,
    CHC_NOTHING,
    CHC_KIND_COUNT
} chc_kind;

typedef struct chc_type chc_type;

CHC_NODISCARD int  chc_type_parse(const char *name, size_t name_len,
                    const chc_alloc *al, chc_type **out, chc_err *err);
void chc_type_destroy(chc_type *t, const chc_alloc *al);

chc_kind     chc_type_kind(const chc_type *t);
size_t       chc_type_n_children(const chc_type *t);
const chc_type *chc_type_child(const chc_type *t, size_t i);

int          chc_type_fixed_size(const chc_type *t);
size_t       chc_type_elem_size(const chc_type *t);
int          chc_type_decimal_precision(const chc_type *t);
int          chc_type_decimal_scale(const chc_type *t);
int          chc_type_datetime64_scale(const chc_type *t);
const char  *chc_type_timezone(const chc_type *t, size_t *out_len);
const char  *chc_type_name(const chc_type *t, size_t *out_len);

size_t       chc_type_enum_count(const chc_type *t);
void         chc_type_enum_at(const chc_type *t, size_t i,
                              const char **name, size_t *name_len,
                              int64_t *value);

/* For Tuple types: returns the ith child's field name or NULL when the
 * tuple is anonymous (or i is out of range). NULL on non-Tuple types. */
const char  *chc_type_tuple_field_name(const chc_type *t, size_t i,
                                       size_t *out_len);

/* Reproduce the printable type name into buf. Returns the number of bytes
 * that would have been written (snprintf-style); use to size buf on a
 * second pass when the return value >= buf_len. buf may be NULL when
 * buf_len == 0 (length query). */
size_t       chc_type_format(const chc_type *t, char *buf, size_t buf_len);

/* -------------------------------------------------------------------------- */
/* Columns                                                                    */
/* -------------------------------------------------------------------------- */

typedef enum chc_col_kind {
    CHC_COL_FIXED = 1,
    CHC_COL_STRING,
    CHC_COL_NULLABLE,
    CHC_COL_ARRAY,
    CHC_COL_TUPLE,
    CHC_COL_LOW_CARDINALITY,
    CHC_COL_NOTHING
} chc_col_kind;

typedef struct chc_column chc_column;

chc_col_kind chc_column_layout(const chc_column *c);
size_t       chc_column_n_rows(const chc_column *c);

/* FIXED. Contiguous n_rows * (*elem_size) bytes, little-endian on the wire.
 * Caller swaps to host order if interpreting as a multi-byte host integer
 * & host is big-endian. */
const void     *chc_column_fixed_data(const chc_column *c, size_t *elem_size);

/* STRING. Row i's bytes are at data + (i == 0 ? 0 : offsets[i-1]) ..
 *                              data + offsets[i].
 * Offsets are exclusive ends, host-byte-order. */
const uint8_t  *chc_column_string_data(const chc_column *c);
const uint64_t *chc_column_string_offsets(const chc_column *c);

/* NULLABLE. null_map[i] == 1 means row i is NULL; inner column always has
 * a value at row i regardless (placeholder zero/empty for nulls). */
const uint8_t    *chc_column_null_map(const chc_column *c);
const chc_column *chc_column_nullable_inner(const chc_column *c);

/* ARRAY. offsets[i] is the cumulative end of row i in the values column.
 * Map decodes as ARRAY whose values column is TUPLE(K, V). Offsets in
 * host byte order. */
const uint64_t   *chc_column_array_offsets(const chc_column *c);
const chc_column *chc_column_array_values(const chc_column *c);

/* TUPLE. All children share the same row count as the tuple itself. */
size_t            chc_column_tuple_arity(const chc_column *c);
const chc_column *chc_column_tuple_child(const chc_column *c, size_t i);

/* LOW_CARDINALITY. key_size is 1/2/4/8. keys is n_rows * key_size, host
 * byte order (swapped at decode time on BE). Dict is a column of the inner
 * type; dict slot 0 is the default value, NULLs in LC(Nullable(T)) ride at
 * dict slot 0 of the inner Nullable. */
int               chc_column_lc_key_size(const chc_column *c);
const void       *chc_column_lc_keys(const chc_column *c);
const chc_column *chc_column_lc_dict(const chc_column *c);

/* Walk a column tree & enforce cross-field invariants the server itself
 * enforces on its native deserialization path:
 *   - Array offsets non-decreasing (SerializationArray.cpp:444, throws
 *     "Arrays offsets are not monotonically increasing")
 *   - LowCardinality keys < dict size (ColumnLowCardinality.cpp:255, throws
 *     "Index for LowCardinality is out of range")
 * chc_block_read does NOT call this automatically — a peer that forges
 * offsets or LC keys can cause callers to read past inner-column bounds.
 * Consumers ingesting from untrusted senders should call this on each
 * block column before traversing it. Returns CHC_OK on success, or
 * CHC_ERR_PROTOCOL with a reason in err on the first violation. NULL c
 * is treated as OK. */
CHC_NODISCARD int chc_column_validate(const chc_column *c, chc_err *err);

/* -------------------------------------------------------------------------- */
/* Block reader                                                               */
/* -------------------------------------------------------------------------- */

typedef struct chc_block chc_block;

typedef struct chc_block_opts {
    /* TCP path (server_revision >= 51903): an 8-byte BlockInfo prefix is on
     * the wire before num_columns. clickhouse-local does not emit it. */
    bool has_block_info;

    /* TCP path (server_revision >= 54454): a 1-byte has_custom_serialization
     * flag follows each column's type name. clickhouse-local does not emit
     * it. */
    bool has_custom_serialization;

    /* Internal read-buffer size. 0 = default (8 KiB). */
    size_t read_buffer_bytes;
} chc_block_opts;

/* Read one block. On clean EOF at a block boundary (no bytes consumed),
 * returns 0 with *out = NULL. On error returns CHC_ERR_* and fills err. */
CHC_NODISCARD int  chc_block_read(chc_io *io, const chc_alloc *al,
                    const chc_block_opts *opts,
                    chc_block **out, chc_err *err);

void chc_block_destroy(chc_block *b, const chc_alloc *al);

size_t            chc_block_n_rows(const chc_block *b);
size_t            chc_block_n_columns(const chc_block *b);
const char       *chc_block_column_name(const chc_block *b, size_t i, size_t *out_len);
const chc_type   *chc_block_column_type(const chc_block *b, size_t i);
const chc_column *chc_block_column(const chc_block *b, size_t i);

/* BlockInfo accessors. Defined-but-zero when opts.has_block_info == false. */
bool    chc_block_is_overflows(const chc_block *b);
int32_t chc_block_bucket_num(const chc_block *b);

/* -------------------------------------------------------------------------- */
/* Block writer                                                               */
/* -------------------------------------------------------------------------- */

typedef struct chc_block_builder chc_block_builder;

CHC_NODISCARD int  chc_block_builder_init(chc_block_builder **out, const chc_alloc *al,
                            chc_err *err);
void chc_block_builder_destroy(chc_block_builder *bb);

/* For variable-length columns, offsets[i] is the cumulative end of row i
 * (exclusive ends, host byte order). For fixed columns, data is n_rows *
 * elem_size little-endian bytes. None of the slabs are copied; they must
 * outlive chc_block_write. */
CHC_NODISCARD int  chc_block_builder_append_fixed(chc_block_builder *bb,
                                    const char *name, size_t name_len,
                                    const chc_type *t,
                                    const void *data, size_t n_rows,
                                    chc_err *err);

CHC_NODISCARD int  chc_block_builder_append_string(chc_block_builder *bb,
                                     const char *name, size_t name_len,
                                     const uint64_t *offsets,
                                     const uint8_t *data, size_t n_rows,
                                     chc_err *err);

/* Composite append helpers. Slabs stay caller-owned; the builder never
 * copies. Offsets / keys are host byte order; the writer byte-swaps to
 * little-endian on BE hosts. `t` carries the column's full CH type and
 * must match the helper variant (e.g. _nullable_fixed expects
 * Nullable(<fixed>), _array_string expects Array(String), and
 * _low_cardinality_string expects LowCardinality(String) or
 * LowCardinality(Nullable(String))).
 *
 * Nested arrays (Array(Array(T))) and Tuple columns are not exposed yet —
 * add when a consumer asks. */
CHC_NODISCARD int  chc_block_builder_append_nullable_fixed(
        chc_block_builder *bb,
        const char *name, size_t name_len,
        const chc_type *t,
        const uint8_t *null_map,
        const void    *inner_data,
        size_t         n_rows, chc_err *err);

CHC_NODISCARD int  chc_block_builder_append_nullable_string(
        chc_block_builder *bb,
        const char *name, size_t name_len,
        const chc_type *t,
        const uint8_t  *null_map,
        const uint64_t *inner_offsets,
        const uint8_t  *inner_data,
        size_t          n_rows, chc_err *err);

CHC_NODISCARD int  chc_block_builder_append_array_fixed(
        chc_block_builder *bb,
        const char *name, size_t name_len,
        const chc_type *t,
        const uint64_t *offsets,
        const void     *values,
        size_t          n_rows, chc_err *err);

CHC_NODISCARD int  chc_block_builder_append_array_string(
        chc_block_builder *bb,
        const char *name, size_t name_len,
        const chc_type *t,
        const uint64_t *offsets,
        const uint64_t *values_offsets,
        const uint8_t  *values_data,
        size_t          n_rows, chc_err *err);

/* Nested Array(Array(...(<fixed/string>))) variants. `t` is top-level
 * Array type, `ndim` is nesting depth (must match `t`). level_offsets
 * is ndim cumulative-end arrays ordered outer-to-inner, level_offsets_len
 * gives count at each level. n_rows is top-level row count, must equal
 * level_offsets_len[0] */
CHC_NODISCARD int  chc_block_builder_append_array_nested_fixed(
        chc_block_builder *bb,
        const char *name, size_t name_len,
        const chc_type *t,
        int                       ndim,
        const uint64_t * const   *level_offsets,
        const size_t             *level_offsets_len,
        const void               *values,
        size_t                    n_rows, chc_err *err);

CHC_NODISCARD int  chc_block_builder_append_array_nested_string(
        chc_block_builder *bb,
        const char *name, size_t name_len,
        const chc_type *t,
        int                       ndim,
        const uint64_t * const   *level_offsets,
        const size_t             *level_offsets_len,
        const uint64_t           *values_offsets,
        const uint8_t            *values_data,
        size_t                    n_rows, chc_err *err);

/* LowCardinality(String) or LowCardinality(Nullable(String)). For the
 * Nullable variant the caller must place a null-sentinel entry at dict
 * index 0 (CH convention) and use key 0 for null rows. */
/* JSON column, STRING serialization. `t` must be CHC_JSON. Rows are JSON
 * document text, one per offset; builder emits an 8-byte LE serialization-
 * version prefix (value 1) once before the same wire format as
 * chc_block_builder_append_string. Caller is responsible for the input
 * being valid JSON; server rejects malformed documents at INSERT time. */
CHC_NODISCARD int  chc_block_builder_append_json_string(
        chc_block_builder *bb,
        const char *name, size_t name_len,
        const chc_type *t,                /* CHC_JSON */
        const uint64_t *offsets,
        const uint8_t  *data,
        size_t n_rows, chc_err *err);

CHC_NODISCARD int  chc_block_builder_append_low_cardinality_string(
        chc_block_builder *bb,
        const char *name, size_t name_len,
        const chc_type *t,
        int             key_size,
        const void     *keys,
        const uint64_t *dict_offsets,
        const uint8_t  *dict_data,
        size_t          dict_n,
        size_t          n_rows, chc_err *err);

CHC_NODISCARD int  chc_block_write(chc_io *io, const chc_block_builder *bb,
                     const chc_block_opts *opts, chc_err *err);

/* ========================================================================== */
/* Implementation                                                             */
/* ========================================================================== */

#ifdef CHC_IMPLEMENTATION

#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

/* Endianness detection.  CH wire format is little-endian. On BE hosts the
 * library byte-swaps the offsets/keys arrays it exposes through host-typed
 * pointers; FIXED slabs stay LE on the wire and the caller swaps when
 * interpreting. */
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#  define CHC_BIG_ENDIAN 1
#else
#  define CHC_BIG_ENDIAN 0
#endif

#if CHC_BIG_ENDIAN
static inline uint16_t chc__bswap16(uint16_t v) {
    return (uint16_t) ((v >> 8) | (v << 8));
}
static inline uint32_t chc__bswap32(uint32_t v) {
    return  ((v & 0xff000000u) >> 24) | ((v & 0x00ff0000u) >>  8)
          | ((v & 0x0000ff00u) <<  8) | ((v & 0x000000ffu) << 24);
}
static inline uint64_t chc__bswap64(uint64_t v) {
    return  ((v & 0xff00000000000000ull) >> 56) | ((v & 0x00ff000000000000ull) >> 40)
          | ((v & 0x0000ff0000000000ull) >> 24) | ((v & 0x000000ff00000000ull) >>  8)
          | ((v & 0x00000000ff000000ull) <<  8) | ((v & 0x0000000000ff0000ull) << 24)
          | ((v & 0x000000000000ff00ull) << 40) | ((v & 0x00000000000000ffull) << 56);
}
#else
#  define chc__bswap16(v) (v)
#  define chc__bswap32(v) (v)
#  define chc__bswap64(v) (v)
#endif

/* -------- CityHash short-string helpers ---------- */

/* Frozen v1.0.3 variant of CityHash, ported from city.cc.
 * Original: Copyright (c) 2011 Google, Inc. (MIT licence).
 * Short-string path lives here so chc__name_to_kind can reuse it; the
 * 128-bit driver used by compressed-frame checksums sits in
 * clickhouse-compression.h and builds on these helpers. */

static uint64_t chc__city_fetch64(const char *p) CHC_REPRODUCIBLE
{
    uint64_t v;
    memcpy(&v, p, 8);
#if CHC_BIG_ENDIAN
    v = chc__bswap64(v);
#endif
    return v;
}

static uint32_t chc__city_fetch32(const char *p) CHC_REPRODUCIBLE
{
    uint32_t v;
    memcpy(&v, p, 4);
#if CHC_BIG_ENDIAN
    v = chc__bswap32(v);
#endif
    return v;
}

static const uint64_t chc__city_k0 = 0xc3a5c85c97cb3127ULL;
static const uint64_t chc__city_k1 = 0xb492b66fbe98f273ULL;
static const uint64_t chc__city_k2 = 0x9ae16a3b2f90404fULL;
static const uint64_t chc__city_k3 = 0xc949d7c7509e6557ULL;

static uint64_t chc__city_rotate_at_least_1(uint64_t v, int s) CHC_UNSEQUENCED
{
    return (v >> s) | (v << (64 - s));
}

static uint64_t chc__city_shift_mix(uint64_t v) CHC_UNSEQUENCED { return v ^ (v >> 47); }

static uint64_t chc__city_hash128_to_64(uint64_t lo, uint64_t hi) CHC_UNSEQUENCED
{
    const uint64_t kMul = 0x9ddfea08eb382d69ULL;
    uint64_t a = (lo ^ hi) * kMul;
    a ^= (a >> 47);
    uint64_t b = (hi ^ a) * kMul;
    b ^= (b >> 47);
    b *= kMul;
    return b;
}

static uint64_t chc__city_hash_len_16(uint64_t u, uint64_t v) CHC_UNSEQUENCED
{
    return chc__city_hash128_to_64(u, v);
}

static uint64_t chc__city_hash_len_0_to_16(const char *s, size_t len) CHC_REPRODUCIBLE
{
    if (len > 8) {
        uint64_t a = chc__city_fetch64(s);
        uint64_t b = chc__city_fetch64(s + len - 8);
        return chc__city_hash_len_16(a,
            chc__city_rotate_at_least_1(b + len, (int) len)) ^ b;
    }
    if (len >= 4) {
        uint64_t a = chc__city_fetch32(s);
        return chc__city_hash_len_16(len + (a << 3),
            chc__city_fetch32(s + len - 4));
    }
    if (len > 0) {
        uint8_t a = (uint8_t) s[0];
        uint8_t b = (uint8_t) s[len >> 1];
        uint8_t c = (uint8_t) s[len - 1];
        uint32_t y = (uint32_t) a + ((uint32_t) b << 8);
        uint32_t z = (uint32_t) len + ((uint32_t) c << 2);
        return chc__city_shift_mix(
                   (uint64_t) y * chc__city_k2 ^
                   (uint64_t) z * chc__city_k3) * chc__city_k2;
    }
    return chc__city_k2;
}

/* -------- error helpers ---------- */

#if defined(__GNUC__) || defined(__clang__)
#  define CHC__PRINTF_FMT(fmt_idx, va_idx) \
    __attribute__((format(printf, fmt_idx, va_idx)))
#else
#  define CHC__PRINTF_FMT(fmt_idx, va_idx)
#endif

static int CHC__PRINTF_FMT(3, 4)
chc__err_set(chc_err *e, int code, const char *fmt, ...)
{
    if (!e) return code;
    e->code = code;
    if (fmt) {
        va_list ap;
        __builtin_va_start(ap, fmt);
        vsnprintf(e->msg, sizeof e->msg, fmt, ap);
        __builtin_va_end(ap);
    } else {
        e->msg[0] = '\0';
    }
    return code;
}

/* -------- alloc helpers ---------- */

static void *
chc__alloc(const chc_alloc *al, size_t n, chc_err *err)
{
    void *p = al->alloc(al->ud, n);
    if (!p) {
        chc__err_set(err, CHC_ERR_OOM, "alloc(%zu) failed", n);
        return NULL;
    }
    return p;
}

static void *
chc__calloc(const chc_alloc *al, size_t n, chc_err *err)
{
    void *p = chc__alloc(al, n, err);
    if (p) memset(p, 0, n);
    return p;
}

static void *
chc__realloc(const chc_alloc *al, void *p, size_t old_n, size_t new_n,
             chc_err *err)
{
    void *q = al->realloc(al->ud, p, old_n, new_n);
    if (!q && new_n) {
        chc__err_set(err, CHC_ERR_OOM, "realloc(%zu->%zu) failed", old_n, new_n);
        return NULL;
    }
    return q;
}

/* Overflow-checked size multiply for count*elem allocation sizing. Matches ckd_mul. */
#if defined(__has_builtin)
#  if __has_builtin(__builtin_mul_overflow)
#    define CHC__HAVE_MUL_OVERFLOW 1
#  endif
#elif defined(__GNUC__) && (__GNUC__ >= 5)
#  define CHC__HAVE_MUL_OVERFLOW 1
#endif

static bool
chc__mul_size(size_t a, size_t b, size_t *out)
{
#if defined(CHC__HAVE_CKD_MUL)
    return ckd_mul(out, a, b);
#elif defined(CHC__HAVE_MUL_OVERFLOW)
    return __builtin_mul_overflow(a, b, out);
#else
    *out = a * b;
    return a != 0 && b > (size_t) SIZE_MAX / a;
#endif
}

static char *
chc__strdup(const chc_alloc *al, const char *s, size_t n, chc_err *err)
{
    char *p = chc__alloc(al, n + 1, err);
    if (!p) return NULL;
    if (n) memcpy(p, s, n);
    p[n] = '\0';
    return p;
}

/* Copy a quoted identifier body (between the outer quote chars), resolving
 * the two escape forms ClickHouse's lexer accepts: a doubled quote stands
 * for one literal quote (`` `` `` -> `` ` ``, `""` -> `"`), & `\X` keeps X
 * verbatim. n is an upper bound; the resolved length is returned via
 * *out_len. */
static char *
chc__strdup_unquote(const chc_alloc *al, const char *s, size_t n, char quote,
                    size_t *out_len, chc_err *err)
{
    /* n is an upper bound; escapes shrink it */
    size_t o = 0;
    for (size_t i = 0; i < n; i++) {
        if (s[i] == '\\' && i + 1 < n) { i++; o++; continue; }
        if (s[i] == quote && i + 1 < n && s[i + 1] == quote) { i++; o++; continue; }
        o++;
    }
    char *p = chc__alloc(al, o + 1, err);
    if (!p) return NULL;
    size_t j = 0;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (c == '\\' && i + 1 < n) { p[j++] = s[++i]; continue; }
        if (c == quote && i + 1 < n && s[i + 1] == quote) { p[j++] = quote; i++; continue; }
        p[j++] = c;
    }
    p[o] = '\0';
    *out_len = o;
    return p;
}

#ifdef CHC_PROVIDE_STDLIB_ALLOC
#include <stdlib.h>
static void *chc__std_alloc(CHC_MAYBE_UNUSED void *ud, size_t n)
    { return malloc(n); }
static void *chc__std_realloc(CHC_MAYBE_UNUSED void *ud, void *p,
                              CHC_MAYBE_UNUSED size_t o, size_t n)
    { return realloc(p, n); }
static void  chc__std_free(CHC_MAYBE_UNUSED void *ud, void *p,
                           CHC_MAYBE_UNUSED size_t b)
{
#if defined(__STDC_VERSION_STDLIB_H__) \
    && __STDC_VERSION_STDLIB_H__ >= 202311L
    free_sized(p, b);
#else
    free(p);
#endif
}
chc_alloc chc_alloc_stdlib(void) {
    chc_alloc a = { NULL, chc__std_alloc, chc__std_realloc, chc__std_free };
    return a;
}
#endif

/* -------- buffered reader ---------- */

#ifndef CHC_READ_BUFFER
#define CHC_READ_BUFFER 8192
#endif

/* Mirror ClickHouse's limits:
 * https://github.com/ClickHouse/ClickHouse/blob/ef11941cf5a/src/IO/ReadHelpers.h#L38
 * https://github.com/ClickHouse/ClickHouse/blob/ef11941cf5a/src/Core/Defines.h#L156-L158
 * https://github.com/ClickHouse/ClickHouse/blob/ef11941cf5a/src/DataTypes/DataTypeFixedString.h#L5
 * https://github.com/ClickHouse/ClickHouse/blob/ef11941cf5a/src/DataTypes/DataTypeFactory.cpp#L122-L127 */
#ifndef CHC_MAX_STRING_SIZE
#define CHC_MAX_STRING_SIZE       (1ULL << 30)
#endif
#ifndef CHC_MAX_NUM_COLUMNS
#define CHC_MAX_NUM_COLUMNS       1000000ULL
#endif
#ifndef CHC_MAX_NUM_ROWS
#define CHC_MAX_NUM_ROWS          1000000000000ULL
#endif
#ifndef CHC_MAX_FIXEDSTRING_SIZE
#define CHC_MAX_FIXEDSTRING_SIZE  0xFFFFFFULL
#endif
#ifndef CHC_MAX_TYPE_DEPTH
#define CHC_MAX_TYPE_DEPTH        300
#endif

/* Public typedef in the declarations section; struct body is internal. */
struct chc_in {
    chc_io          *io;        /* NULL => ioless  */
    const chc_alloc *al;
    uint8_t         *buf;
    size_t           cap;
    size_t           pos;       /* read cursor */
    size_t           fill;      /* bytes valid in buf */
    bool             eof;
    uint64_t         consumed;  /* total bytes returned to caller */
    size_t           mark;      /* rewind target; SIZE_MAX when unset */
};

int
chc_in_init(chc_in *in, chc_io *io, const chc_alloc *al,
             size_t cap, chc_err *err)
{
    if (cap == 0) cap = CHC_READ_BUFFER;
    in->io = io;
    in->al = al;
    in->buf = chc__alloc(al, cap, err);
    if (!in->buf) return CHC_ERR_OOM;
    in->cap = cap;
    in->pos = 0;
    in->fill = 0;
    in->eof = false;
    in->consumed = 0;
    in->mark = SIZE_MAX;
    return CHC_OK;
}

int
chc_in_init_ioless(chc_in *in, const chc_alloc *al)
{
    in->io = NULL;
    in->al = al;
    in->buf = NULL;             /* grown lazily on first feed */
    in->cap = 0;
    in->pos = 0;
    in->fill = 0;
    in->eof = false;
    in->consumed = 0;
    in->mark = SIZE_MAX;
    return CHC_OK;
}

/* Drop prefix [0, keep): keep = mark when a checkpoint is live, else pos.
 * consumed counts returned bytes, not offsets, so compaction leaves it be;
 * mark and pos shift together so (pos - mark) survives for rewind. */
static void
chc__in_compact(chc_in *in)
{
    size_t keep = in->mark == SIZE_MAX ? in->pos : in->mark;
    if (keep == 0) return;
    size_t live = in->fill - keep;
    if (live) memmove(in->buf, in->buf + keep, live);
    in->fill -= keep;
    in->pos  -= keep;
    if (in->mark != SIZE_MAX) in->mark -= keep;
}

int
chc_in_submit(chc_in *in, const void *buf, size_t len, chc_err *err)
{
    if (in->io)
        return chc__err_set(err, CHC_ERR_USAGE, "chc_in_submit on io-backed reader");
    if (len == 0) return CHC_OK;
    chc__in_compact(in);
    if (in->fill + len > in->cap) {
        size_t ncap = in->cap ? in->cap : CHC_READ_BUFFER;
        while (ncap < in->fill + len) {
            if (ncap > SIZE_MAX / 2) { ncap = in->fill + len; break; }
            ncap *= 2;
        }
        uint8_t *nb = chc__realloc(in->al, in->buf, in->cap, ncap, err);
        if (!nb) return CHC_ERR_OOM;
        in->buf = nb;
        in->cap = ncap;
    }
    memcpy(in->buf + in->fill, buf, len);
    in->fill += len;
    return CHC_OK;
}

size_t
chc_in_available(const chc_in *in)
{
    return in->fill - in->pos;
}

void
chc_in_reset(chc_in *in)
{
    /* Drop consumed bytes [0, pos), keep [pos, fill). Clears any checkpoint;
     * callers reset at packet boundaries, never mid-parse. */
    in->mark = SIZE_MAX;
    if (in->pos == 0) return;
    size_t live = in->fill - in->pos;
    if (live) memmove(in->buf, in->buf + in->pos, live);
    in->fill = live;
    in->pos = 0;
}

void
chc_in_free(chc_in *in)
{
    in->al->free(in->al->ud, in->buf, in->cap);
    in->buf = NULL;
}

/* Mark read cursor as rewind target. Ioless checkpoints at a packet
 * boundary so a mid-parse CHC_WOULD_BLOCK can rewind and re-parse once more bytes arrive. */
CHC_MAYBE_UNUSED static void
chc__in_checkpoint(chc_in *in)
{
    in->mark = in->pos;
}

/* Restore the cursor to the last checkpoint, un-counting bytes consumed
 * since it (so consumed stays equal to an io-backed read of the same
 * stream). Clears the mark. */
CHC_MAYBE_UNUSED static int
chc__in_rewind(chc_in *in)
{
    if (in->mark == SIZE_MAX)
        return CHC_ERR_USAGE;
    in->consumed -= in->pos - in->mark;
    in->pos = in->mark;
    in->mark = SIZE_MAX;
    return CHC_OK;
}

/* Refill buf with at least one byte. Returns 0 on success, CHC_ERR_EOF on
 * clean EOF, CHC_ERR_IO/CANCELLED on failure.
 * Returns CHC_WOULD_BLOCK when not CHC_OK in ioless. */
static int
chc__in_refill(chc_in *in, chc_err *err)
{
    if (in->pos < in->fill) return CHC_OK;
    if (in->io == NULL)
        return chc__err_set(err, CHC_WOULD_BLOCK, "ioless buffer drained");
    if (in->eof) return chc__err_set(err, CHC_ERR_EOF, "unexpected eof");

    if (in->io->check_cancel && in->io->check_cancel(in->io->ud))
        return chc__err_set(err, CHC_ERR_CANCELLED, "cancelled");

    in->pos = 0;
    in->fill = 0;
    size_t got = 0;
    int rc = in->io->read(in->io->ud, in->buf, in->cap, &got, err);
    if (rc != CHC_OK) {
        if (err && err->code == 0) chc__err_set(err, CHC_ERR_IO, "read failed");
        return err && err->code ? err->code : CHC_ERR_IO;
    }
    if (got == 0) { in->eof = true; return chc__err_set(err, CHC_ERR_EOF, "unexpected eof"); }
    in->fill = got;
    return CHC_OK;
}

static int
chc__read_byte(chc_in *in, uint8_t *out, chc_err *err)
{
    if (in->pos >= in->fill) {
        int rc = chc__in_refill(in, err);
        if (rc != CHC_OK) return rc;
    }
    *out = in->buf[in->pos++];
    in->consumed++;
    return CHC_OK;
}

static int
chc__read_bytes(chc_in *in, void *dst, size_t n, chc_err *err)
{
    uint8_t *p = dst;

    if (in->pos < in->fill) {
        size_t avail = in->fill - in->pos;
        size_t take = n < avail ? n : avail;
        memcpy(p, in->buf + in->pos, take);
        in->pos += take;
        in->consumed += take;
        p += take;
        n -= take;
    }

    /* Bypass staging buf when request spans more than one refill, read
     * straight into caller's dst to skip the staging memcpy. Only fires
     * after the staging buf is drained, so buffered-reader invariants
     * (pos, fill, consumed) stay consistent. Disabled in ioless: bypassed
     * bytes land outside in->buf and can't be rewound, so ioless routes
     * everything through the (growable) staging buf. */
    while (in->io != NULL && n > in->cap) {
        if (in->eof)
            return chc__err_set(err, CHC_ERR_EOF, "short read");
        if (in->io->check_cancel && in->io->check_cancel(in->io->ud))
            return chc__err_set(err, CHC_ERR_CANCELLED, "cancelled");
        size_t got = 0;
        int rc = in->io->read(in->io->ud, p, n, &got, err);
        if (rc != CHC_OK) {
            if (err && err->code == 0) chc__err_set(err, CHC_ERR_IO, "read failed");
            return err && err->code ? err->code : CHC_ERR_IO;
        }
        if (got == 0) {
            in->eof = true;
            return chc__err_set(err, CHC_ERR_EOF, "short read");
        }
        p += got;
        in->consumed += got;
        n -= got;
    }

    while (n) {
        if (in->pos >= in->fill) {
            int rc = chc__in_refill(in, err);
            if (rc == CHC_ERR_EOF)
                return chc__err_set(err, CHC_ERR_EOF, "short read");
            if (rc != CHC_OK) return rc;
        }
        size_t avail = in->fill - in->pos;
        size_t take = n < avail ? n : avail;
        memcpy(p, in->buf + in->pos, take);
        in->pos += take;
        in->consumed += take;
        p += take;
        n -= take;
    }
    return CHC_OK;
}

static int
chc__read_varuint(chc_in *in, uint64_t *out, chc_err *err)
{
    uint64_t v = 0;
    for (int i = 0; i < 10; i++) {
        uint8_t b;
        int rc = chc__read_byte(in, &b, err);
        if (rc != CHC_OK) return rc;
        v |= ((uint64_t)(b & 0x7f)) << (7 * i);
        if (!(b & 0x80)) { *out = v; return CHC_OK; }
    }
    return chc__err_set(err, CHC_ERR_PROTOCOL, "varint too long");
}

static int
chc__read_u32_le(chc_in *in, uint32_t *out, chc_err *err)
{
    uint8_t b[4];
    int rc = chc__read_bytes(in, b, 4, err);
    if (rc != CHC_OK) return rc;
    *out = (uint32_t) b[0] | ((uint32_t) b[1] << 8)
         | ((uint32_t) b[2] << 16) | ((uint32_t) b[3] << 24);
    return CHC_OK;
}

static int
chc__read_u64_le(chc_in *in, uint64_t *out, chc_err *err)
{
    uint8_t b[8];
    int rc = chc__read_bytes(in, b, 8, err);
    if (rc != CHC_OK) return rc;
    *out = (uint64_t) b[0]        | ((uint64_t) b[1] << 8)
         | ((uint64_t) b[2] << 16) | ((uint64_t) b[3] << 24)
         | ((uint64_t) b[4] << 32) | ((uint64_t) b[5] << 40)
         | ((uint64_t) b[6] << 48) | ((uint64_t) b[7] << 56);
    return CHC_OK;
}

static int
chc__read_string(chc_in *in, char **out, size_t *out_len, chc_err *err)
{
    const chc_alloc *al = in->al;
    uint64_t len;
    int rc = chc__read_varuint(in, &len, err);
    if (rc != CHC_OK) return rc;
    if (len > CHC_MAX_STRING_SIZE)
        return chc__err_set(err, CHC_ERR_PROTOCOL, "string too long: %llu",
                            (unsigned long long) len);
    char *buf = chc__alloc(al, len + 1, err);
    if (!buf) return CHC_ERR_OOM;
    if (len) {
        rc = chc__read_bytes(in, buf, (size_t) len, err);
        if (rc != CHC_OK) { al->free(al->ud, buf, len + 1); return rc; }
    }
    buf[len] = '\0';
    *out = buf;
    *out_len = (size_t) len;
    return CHC_OK;
}

/* -------- type AST internals ---------- */

struct chc_type {
    chc_kind  kind;
    char     *name;
    size_t    name_len;
    size_t    n_children;
    chc_type **children;
    /* For Tuple: parallel to children[]. NULL when tuple has no field names.
     * Individual slots may be NULL for mixed-anonymity tuples (rare; CH allows it). */
    char    **field_names;
    size_t   *field_name_lens;
    union {
        struct { int n; }                              fixed_string;  /* FixedString(N) */
        struct { int precision, scale; }               decimal;       /* Decimal(P, S) */
        struct { int scale; char *tz; size_t tz_len; } temporal;      /* DateTime / DateTime64 / Time64 */
        struct {
            size_t n;
            struct { char *name; uint32_t name_len; int16_t value; } *items;
        } enum_;
    };
};

static bool chc__kind_is_decimal(chc_kind k)
{ return k == CHC_DECIMAL32 || k == CHC_DECIMAL64 || k == CHC_DECIMAL128 || k == CHC_DECIMAL256; }
static bool chc__kind_is_enum(chc_kind k)
{ return k == CHC_ENUM8 || k == CHC_ENUM16; }
static bool chc__kind_has_tz(chc_kind k)
{ return k == CHC_DATETIME || k == CHC_DATETIME64 || k == CHC_TIME64; }

void
chc_type_destroy(chc_type *t, const chc_alloc *al)
{
    if (!t) return;
    if (t->field_names) {
        for (size_t i = 0; i < t->n_children; i++)
            al->free(al->ud, t->field_names[i], t->field_name_lens[i] + 1);
        al->free(al->ud, t->field_names, t->n_children * sizeof *t->field_names);
    }
    al->free(al->ud, t->field_name_lens, t->n_children * sizeof *t->field_name_lens);
    for (size_t i = 0; i < t->n_children; i++)
        chc_type_destroy(t->children[i], al);
    al->free(al->ud, t->children, t->n_children * sizeof *t->children);
    if (chc__kind_is_enum(t->kind) && t->enum_.items) {
        for (size_t i = 0; i < t->enum_.n; i++)
            al->free(al->ud, t->enum_.items[i].name,
                     t->enum_.items[i].name_len + 1);
        al->free(al->ud, t->enum_.items,
                 t->enum_.n * sizeof *t->enum_.items);
    } else if (chc__kind_has_tz(t->kind))
        al->free(al->ud, t->temporal.tz, t->temporal.tz_len + 1);
    al->free(al->ud, t->name, t->name_len + 1);
    al->free(al->ud, t, sizeof *t);
}

chc_kind         chc_type_kind(const chc_type *t)               { return t ? t->kind : CHC_VOID; }
size_t           chc_type_n_children(const chc_type *t)         { return t ? t->n_children : 0; }
const chc_type  *chc_type_child(const chc_type *t, size_t i)    { return (t && i < t->n_children) ? t->children[i] : NULL; }
int              chc_type_fixed_size(const chc_type *t)         { return t && t->kind == CHC_FIXED_STRING ? t->fixed_string.n : 0; }
int              chc_type_decimal_scale(const chc_type *t)      { return (t && chc__kind_is_decimal(t->kind)) ? t->decimal.scale : 0; }
int              chc_type_datetime64_scale(const chc_type *t)   { return (t && (t->kind == CHC_DATETIME64 || t->kind == CHC_TIME64)) ? t->temporal.scale : 0; }
const char      *chc_type_name(const chc_type *t, size_t *out_len) {
    if (out_len) *out_len = t ? t->name_len : 0;
    return t ? t->name : NULL;
}
const char      *chc_type_timezone(const chc_type *t, size_t *out_len) {
    bool has = t && chc__kind_has_tz(t->kind);
    if (out_len) *out_len = has ? t->temporal.tz_len : 0;
    return has ? t->temporal.tz : NULL;
}
size_t           chc_type_enum_count(const chc_type *t)         { return (t && chc__kind_is_enum(t->kind)) ? t->enum_.n : 0; }
void             chc_type_enum_at(const chc_type *t, size_t i,
                                  const char **name, size_t *name_len,
                                  int64_t *value) {
    if (!t || !chc__kind_is_enum(t->kind) || i >= t->enum_.n) {
        if (name) *name = NULL;
        if (name_len) *name_len = 0;
        if (value) *value = 0;
        return;
    }
    if (name) *name = t->enum_.items[i].name;
    if (name_len) *name_len = t->enum_.items[i].name_len;
    if (value) *value = t->enum_.items[i].value;
}

const char *
chc_type_tuple_field_name(const chc_type *t, size_t i, size_t *out_len)
{
    if (!t || t->kind != CHC_TUPLE || !t->field_names || i >= t->n_children) {
        if (out_len) *out_len = 0;
        return NULL;
    }
    if (out_len) *out_len = t->field_name_lens[i];
    return t->field_names[i];
}

int
chc_type_decimal_precision(const chc_type *t)
{
    if (!t) return 0;
    switch (t->kind) {
    case CHC_DECIMAL32:  return 9;
    case CHC_DECIMAL64:  return 18;
    case CHC_DECIMAL128: return 38;
    case CHC_DECIMAL256: return 76;
    default:             return t->decimal.precision;
    }
}

/* -------- type parser ---------- */

/* Tokens & lexer mirror clickhouse-cpp/types/type_parser.cpp. The parser
 * is structurally identical (recursive on '(' / ')' / ','). */
typedef enum {
    CHC__TOK_EOS = 0, CHC__TOK_NAME, CHC__TOK_NUMBER, CHC__TOK_STRING,
    CHC__TOK_LPAREN, CHC__TOK_RPAREN, CHC__TOK_COMMA, CHC__TOK_EQ,
    CHC__TOK_INVALID
} chc__tok_kind;

typedef struct {
    chc__tok_kind kind;
    const char   *start;
    size_t        len;
    /* For CHC__TOK_NAME: 0 = bare identifier; '`' or '"' = quoted, & start/len
     * span the body between the outer quotes (still raw -- doubled-quote &
     * backslash escapes are resolved when copied out). */
    char          quote;
} chc__tok;

typedef struct {
    const char *cur, *end;
    chc__tok    peeked;
    bool        has_peek;
} chc__lex;

static chc__tok
chc__next_tok(chc__lex *lx)
{
    while (lx->cur < lx->end) {
        char c = *lx->cur;
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') { lx->cur++; continue; }
        const char *st = lx->cur;
        if (c == '(') { lx->cur++; return (chc__tok){CHC__TOK_LPAREN, st, 1, 0}; }
        if (c == ')') { lx->cur++; return (chc__tok){CHC__TOK_RPAREN, st, 1, 0}; }
        if (c == ',') { lx->cur++; return (chc__tok){CHC__TOK_COMMA, st, 1, 0}; }
        if (c == '=') { lx->cur++; return (chc__tok){CHC__TOK_EQ, st, 1, 0}; }
        if (c == '\'') {
            /* single-quoted string; clickhouse-cpp does not escape, so we
             * accept anything up to the next unescaped quote. */
            lx->cur++;
            const char *body = lx->cur;
            while (lx->cur < lx->end && *lx->cur != '\'') lx->cur++;
            if (lx->cur >= lx->end) return (chc__tok){CHC__TOK_INVALID, st, 0, 0};
            size_t blen = (size_t) (lx->cur - body);
            lx->cur++;                                     /* eat closing ' */
            return (chc__tok){CHC__TOK_STRING, body, blen, 0};
        }
        if (c == '`' || c == '"') {
            /* Quoted identifier, matching ClickHouse Lexer.cpp `quotedString`:
             * doubled quote (`` `` `` or `""`) & backslash-escapes are skipped
             * during scanning, resolved at copy time. */
            char q = c;
            lx->cur++;
            const char *body = lx->cur;
            while (lx->cur < lx->end) {
                char d = *lx->cur;
                if (d == '\\') {
                    lx->cur++;
                    if (lx->cur < lx->end) lx->cur++;
                    continue;
                }
                if (d == q) {
                    if (lx->cur + 1 < lx->end && lx->cur[1] == q) {
                        lx->cur += 2;
                        continue;
                    }
                    break;
                }
                lx->cur++;
            }
            if (lx->cur >= lx->end) return (chc__tok){CHC__TOK_INVALID, st, 0, 0};
            size_t blen = (size_t) (lx->cur - body);
            lx->cur++;                                     /* eat closing quote */
            return (chc__tok){CHC__TOK_NAME, body, blen, q};
        }
        if (isalpha((unsigned char) c) || c == '_') {
            while (lx->cur < lx->end) {
                char d = *lx->cur;
                if (!(isalnum((unsigned char) d) || d == '_')) break;
                lx->cur++;
            }
            return (chc__tok){CHC__TOK_NAME, st, (size_t) (lx->cur - st), 0};
        }
        if (isdigit((unsigned char) c) || c == '-') {
            lx->cur++;
            while (lx->cur < lx->end && isdigit((unsigned char) *lx->cur)) lx->cur++;
            return (chc__tok){CHC__TOK_NUMBER, st, (size_t) (lx->cur - st), 0};
        }
        return (chc__tok){CHC__TOK_INVALID, st, 0, 0};
    }
    return (chc__tok){CHC__TOK_EOS, lx->end, 0, 0};
}

static chc__tok
chc__peek_tok(chc__lex *lx)
{
    if (!lx->has_peek) {
        lx->peeked = chc__next_tok(lx);
        lx->has_peek = true;
    }
    return lx->peeked;
}

static chc__tok
chc__eat_tok(chc__lex *lx)
{
    chc__tok t = chc__peek_tok(lx);
    lx->has_peek = false;
    return t;
}

static int64_t
chc__atoi64(const char *s, size_t n)
{
    int64_t v = 0;
    bool neg = false;
    size_t i = 0;
    if (n && s[0] == '-') { neg = true; i = 1; }
    for (; i < n; i++) v = v * 10 + (s[i] - '0');
    return neg ? -v : v;
}

/* AUTO-GENERATED-NAME-TABLE-BEGIN -- regenerate via tools/regen_name_table.sh */
#define CHC__NAME_TABLE_M 256u
#define CHC__NAME_TABLE_SEED 720ull
struct chc__name_row { const char *name; chc_kind kind; };
static const struct chc__name_row chc__name_table[CHC__NAME_TABLE_M] = {
    [  0] = {0},
    [  1] = {0},
    [  2] = {0},
    [  3] = {0},
    [  4] = {"Int32", CHC_INT32},
    [  5] = {0},
    [  6] = {0},
    [  7] = {0},
    [  8] = {"Float32", CHC_FLOAT32},
    [  9] = {0},
    [ 10] = {0},
    [ 11] = {0},
    [ 12] = {0},
    [ 13] = {"MultiPolygon", CHC_MULTI_POLYGON},
    [ 14] = {0},
    [ 15] = {0},
    [ 16] = {0},
    [ 17] = {0},
    [ 18] = {0},
    [ 19] = {0},
    [ 20] = {"DateTime", CHC_DATETIME},
    [ 21] = {"Dynamic", CHC_DYNAMIC},
    [ 22] = {0},
    [ 23] = {0},
    [ 24] = {0},
    [ 25] = {0},
    [ 26] = {0},
    [ 27] = {0},
    [ 28] = {0},
    [ 29] = {0},
    [ 30] = {"IntervalMinute", CHC_INTERVAL},
    [ 31] = {0},
    [ 32] = {0},
    [ 33] = {"Ring", CHC_RING},
    [ 34] = {0},
    [ 35] = {0},
    [ 36] = {"IntervalMicrosecond", CHC_INTERVAL},
    [ 37] = {"Decimal64", CHC_DECIMAL64},
    [ 38] = {0},
    [ 39] = {0},
    [ 40] = {"DateTime64", CHC_DATETIME64},
    [ 41] = {0},
    [ 42] = {0},
    [ 43] = {"Int128", CHC_INT128},
    [ 44] = {"Tuple", CHC_TUPLE},
    [ 45] = {0},
    [ 46] = {0},
    [ 47] = {0},
    [ 48] = {"IntervalDay", CHC_INTERVAL},
    [ 49] = {"Map", CHC_MAP},
    [ 50] = {"IntervalSecond", CHC_INTERVAL},
    [ 51] = {0},
    [ 52] = {"UInt8", CHC_UINT8},
    [ 53] = {0},
    [ 54] = {0},
    [ 55] = {"Enum16", CHC_ENUM16},
    [ 56] = {0},
    [ 57] = {"IntervalMillisecond", CHC_INTERVAL},
    [ 58] = {0},
    [ 59] = {0},
    [ 60] = {"Int8", CHC_INT8},
    [ 61] = {0},
    [ 62] = {0},
    [ 63] = {0},
    [ 64] = {0},
    [ 65] = {"IntervalHour", CHC_INTERVAL},
    [ 66] = {0},
    [ 67] = {0},
    [ 68] = {"UInt256", CHC_UINT256},
    [ 69] = {0},
    [ 70] = {0},
    [ 71] = {0},
    [ 72] = {0},
    [ 73] = {"Date32", CHC_DATE32},
    [ 74] = {"BFloat16", CHC_BFLOAT16},
    [ 75] = {0},
    [ 76] = {0},
    [ 77] = {0},
    [ 78] = {0},
    [ 79] = {0},
    [ 80] = {0},
    [ 81] = {0},
    [ 82] = {0},
    [ 83] = {"Nullable", CHC_NULLABLE},
    [ 84] = {0},
    [ 85] = {0},
    [ 86] = {0},
    [ 87] = {0},
    [ 88] = {0},
    [ 89] = {"IntervalMonth", CHC_INTERVAL},
    [ 90] = {0},
    [ 91] = {0},
    [ 92] = {0},
    [ 93] = {0},
    [ 94] = {0},
    [ 95] = {0},
    [ 96] = {0},
    [ 97] = {0},
    [ 98] = {0},
    [ 99] = {0},
    [100] = {0},
    [101] = {"UInt128", CHC_UINT128},
    [102] = {0},
    [103] = {0},
    [104] = {0},
    [105] = {0},
    [106] = {"Enum8", CHC_ENUM8},
    [107] = {0},
    [108] = {0},
    [109] = {0},
    [110] = {0},
    [111] = {"Void", CHC_VOID},
    [112] = {0},
    [113] = {0},
    [114] = {0},
    [115] = {"IPv4", CHC_IPV4},
    [116] = {0},
    [117] = {0},
    [118] = {0},
    [119] = {0},
    [120] = {"Variant", CHC_VARIANT},
    [121] = {"LowCardinality", CHC_LOW_CARDINALITY},
    [122] = {"Time64", CHC_TIME64},
    [123] = {"Decimal128", CHC_DECIMAL128},
    [124] = {0},
    [125] = {0},
    [126] = {0},
    [127] = {0},
    [128] = {0},
    [129] = {0},
    [130] = {"UInt64", CHC_UINT64},
    [131] = {0},
    [132] = {"UInt32", CHC_UINT32},
    [133] = {"Int16", CHC_INT16},
    [134] = {"JSON", CHC_JSON},
    [135] = {"SimpleAggregateFunction", CHC_SIMPLE_AGGREGATE_FUNCTION},
    [136] = {"IntervalNanosecond", CHC_INTERVAL},
    [137] = {0},
    [138] = {0},
    [139] = {0},
    [140] = {0},
    [141] = {0},
    [142] = {0},
    [143] = {0},
    [144] = {0},
    [145] = {0},
    [146] = {0},
    [147] = {0},
    [148] = {0},
    [149] = {0},
    [150] = {"Nothing", CHC_NOTHING},
    [151] = {"Date", CHC_DATE},
    [152] = {0},
    [153] = {0},
    [154] = {0},
    [155] = {0},
    [156] = {0},
    [157] = {"IPv6", CHC_IPV6},
    [158] = {0},
    [159] = {0},
    [160] = {0},
    [161] = {0},
    [162] = {0},
    [163] = {0},
    [164] = {0},
    [165] = {0},
    [166] = {0},
    [167] = {0},
    [168] = {"Array", CHC_ARRAY},
    [169] = {0},
    [170] = {0},
    [171] = {0},
    [172] = {"Time", CHC_TIME},
    [173] = {0},
    [174] = {0},
    [175] = {0},
    [176] = {0},
    [177] = {"Object", CHC_OBJECT},
    [178] = {"Decimal32", CHC_DECIMAL32},
    [179] = {0},
    [180] = {0},
    [181] = {0},
    [182] = {0},
    [183] = {"Decimal256", CHC_DECIMAL256},
    [184] = {0},
    [185] = {0},
    [186] = {0},
    [187] = {0},
    [188] = {0},
    [189] = {"UUID", CHC_UUID},
    [190] = {0},
    [191] = {0},
    [192] = {0},
    [193] = {0},
    [194] = {0},
    [195] = {0},
    [196] = {0},
    [197] = {0},
    [198] = {0},
    [199] = {0},
    [200] = {0},
    [201] = {0},
    [202] = {0},
    [203] = {0},
    [204] = {0},
    [205] = {0},
    [206] = {"Nested", CHC_NESTED},
    [207] = {0},
    [208] = {0},
    [209] = {0},
    [210] = {0},
    [211] = {"Polygon", CHC_POLYGON},
    [212] = {0},
    [213] = {0},
    [214] = {"String", CHC_STRING},
    [215] = {0},
    [216] = {0},
    [217] = {0},
    [218] = {"AggregateFunction", CHC_AGGREGATE_FUNCTION},
    [219] = {"Int256", CHC_INT256},
    [220] = {0},
    [221] = {0},
    [222] = {0},
    [223] = {"UInt16", CHC_UINT16},
    [224] = {"IntervalQuarter", CHC_INTERVAL},
    [225] = {0},
    [226] = {0},
    [227] = {0},
    [228] = {0},
    [229] = {0},
    [230] = {0},
    [231] = {0},
    [232] = {"Bool", CHC_BOOL},
    [233] = {0},
    [234] = {0},
    [235] = {0},
    [236] = {"FixedString", CHC_FIXED_STRING},
    [237] = {"Int64", CHC_INT64},
    [238] = {0},
    [239] = {0},
    [240] = {0},
    [241] = {0},
    [242] = {0},
    [243] = {0},
    [244] = {0},
    [245] = {"IntervalYear", CHC_INTERVAL},
    [246] = {"Float64", CHC_FLOAT64},
    [247] = {0},
    [248] = {0},
    [249] = {0},
    [250] = {0},
    [251] = {0},
    [252] = {0},
    [253] = {"IntervalWeek", CHC_INTERVAL},
    [254] = {"Point", CHC_POINT},
    [255] = {0},
};
/* AUTO-GENERATED-NAME-TABLE-END */

/* Plain "Decimal" is intentionally absent from the table; the parser's
 * decimal_alias branch resolves it from precision. Miss -> CHC_VOID, also
 * the sentinel for unknown names; caller disambiguates with an explicit
 * memcmp against "Void". */
static chc_kind
chc__name_to_kind(const char *s, size_t n) CHC_REPRODUCIBLE
{
    if (n == 0 || n > 23) return CHC_VOID;
    size_t h_len = n < 16 ? n : 16;
    uint64_t h = chc__city_hash_len_16(
        chc__city_hash_len_0_to_16(s, h_len) + (uint64_t) n,
        CHC__NAME_TABLE_SEED);
    const struct chc__name_row *r = &chc__name_table[h & (CHC__NAME_TABLE_M - 1)];
    if (r->name && strlen(r->name) == n && memcmp(r->name, s, n) == 0)
        return r->kind;
    return CHC_VOID;
}

static int chc__parse_type(chc__lex *lx, const chc_alloc *al,
                           const char *whole_start, const char *whole_end,
                           size_t depth, chc_type **out, chc_err *err);

/* Append a child pointer to parent's children array. */
static int
chc__type_push_child(const chc_alloc *al, chc_type *parent, chc_type *child,
                     chc_err *err)
{
    size_t n = parent->n_children;
    chc_type **arr = chc__realloc(al, parent->children,
                                  n * sizeof *arr, (n + 1) * sizeof *arr, err);
    if (!arr) return CHC_ERR_OOM;
    arr[n] = child;
    parent->children = arr;
    parent->n_children = n + 1;
    return CHC_OK;
}

static int
chc__type_push_enum(const chc_alloc *al, chc_type *parent,
                    const char *name, size_t name_len, int64_t value,
                    chc_err *err)
{
    size_t n = parent->enum_.n;
    void *arr = chc__realloc(al, parent->enum_.items,
                             n * sizeof *parent->enum_.items,
                             (n + 1) * sizeof *parent->enum_.items, err);
    if (!arr) return CHC_ERR_OOM;
    parent->enum_.items = arr;
    parent->enum_.items[n].name = chc__strdup(al, name, name_len, err);
    if (!parent->enum_.items[n].name) return CHC_ERR_OOM;
    parent->enum_.items[n].name_len = name_len;
    parent->enum_.items[n].value = value;
    parent->enum_.n = n + 1;
    return CHC_OK;
}

static int
chc__parse_type(chc__lex *lx, const chc_alloc *al,
                const char *whole_start, const char *whole_end,
                size_t depth, chc_type **out, chc_err *err)
{
    if (depth > CHC_MAX_TYPE_DEPTH)
        return chc__err_set(err, CHC_ERR_TYPE,
            "type nested too deeply (max %llu)",
            (unsigned long long) CHC_MAX_TYPE_DEPTH);

    chc__tok head = chc__eat_tok(lx);
    if (head.kind != CHC__TOK_NAME || head.quote)
        return chc__err_set(err, CHC_ERR_TYPE, "expected type name");

    chc_type *t = chc__calloc(al, sizeof *t, err);
    if (!t) return CHC_ERR_OOM;
    bool decimal_alias = (head.len == 7 && memcmp(head.start, "Decimal", 7) == 0);
    if (decimal_alias) {
        t->kind = CHC_DECIMAL128;       /* placeholder; refined from precision */
    } else {
        t->kind = chc__name_to_kind(head.start, head.len);
        if (t->kind == CHC_VOID && !(head.len == 4 && memcmp(head.start, "Void", 4) == 0)) {
            chc_type_destroy(t, al);
            return chc__err_set(err, CHC_ERR_TYPE, "unknown type: %.*s",
                                (int) head.len, head.start);
        }
    }

    const char *name_start = head.start;
    const char *name_end   = head.start + head.len;

    /* Optional parameter list. */
    if (chc__peek_tok(lx).kind == CHC__TOK_LPAREN) {
        chc__eat_tok(lx);

        if (t->kind == CHC_ENUM8 || t->kind == CHC_ENUM16) {
            /* 'name' = value, 'name' = value, ... */
            while (chc__peek_tok(lx).kind != CHC__TOK_RPAREN) {
                chc__tok s = chc__eat_tok(lx);
                if (s.kind != CHC__TOK_STRING) {
                    chc_type_destroy(t, al);
                    return chc__err_set(err, CHC_ERR_TYPE, "Enum: expected quoted name");
                }
                chc__tok eq = chc__eat_tok(lx);
                if (eq.kind != CHC__TOK_EQ) {
                    chc_type_destroy(t, al);
                    return chc__err_set(err, CHC_ERR_TYPE, "Enum: expected '='");
                }
                chc__tok num = chc__eat_tok(lx);
                if (num.kind != CHC__TOK_NUMBER) {
                    chc_type_destroy(t, al);
                    return chc__err_set(err, CHC_ERR_TYPE, "Enum: expected value");
                }
                int rc = chc__type_push_enum(al, t, s.start, s.len,
                                             chc__atoi64(num.start, num.len), err);
                if (rc != CHC_OK) { chc_type_destroy(t, al); return rc; }
                if (chc__peek_tok(lx).kind == CHC__TOK_COMMA) chc__eat_tok(lx);
            }
        } else if (t->kind == CHC_FIXED_STRING) {
            chc__tok num = chc__eat_tok(lx);
            if (num.kind != CHC__TOK_NUMBER) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "FixedString: expected N");
            }
            int64_t n = chc__atoi64(num.start, num.len);
            if (n <= 0 || (uint64_t) n > CHC_MAX_FIXEDSTRING_SIZE) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE,
                    "FixedString: N out of range: %lld", (long long) n);
            }
            t->fixed_string.n = (int) n;
        } else if (decimal_alias) {
            chc__tok np = chc__eat_tok(lx);
            if (np.kind != CHC__TOK_NUMBER) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "Decimal: expected precision");
            }
            chc__tok cm = chc__eat_tok(lx);
            if (cm.kind != CHC__TOK_COMMA) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "Decimal: expected ','");
            }
            chc__tok ns = chc__eat_tok(lx);
            if (ns.kind != CHC__TOK_NUMBER) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "Decimal: expected scale");
            }
            int prec = (int) chc__atoi64(np.start, np.len);
            t->decimal.precision = prec;
            t->decimal.scale = (int) chc__atoi64(ns.start, ns.len);
            if      (prec <=  9) t->kind = CHC_DECIMAL32;
            else if (prec <= 18) t->kind = CHC_DECIMAL64;
            else if (prec <= 38) t->kind = CHC_DECIMAL128;
            else                 t->kind = CHC_DECIMAL256;
        } else if (t->kind == CHC_DECIMAL32 || t->kind == CHC_DECIMAL64
                || t->kind == CHC_DECIMAL128 || t->kind == CHC_DECIMAL256) {
            chc__tok num = chc__eat_tok(lx);
            if (num.kind != CHC__TOK_NUMBER) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "Decimal: expected scale");
            }
            t->decimal.scale = (int) chc__atoi64(num.start, num.len);
        } else if (t->kind == CHC_DATETIME64 || t->kind == CHC_TIME64) {
            chc__tok num = chc__eat_tok(lx);
            if (num.kind != CHC__TOK_NUMBER) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "DateTime64: expected precision");
            }
            t->temporal.scale = (int) chc__atoi64(num.start, num.len);
            if (chc__peek_tok(lx).kind == CHC__TOK_COMMA) {
                chc__eat_tok(lx);
                chc__tok s = chc__eat_tok(lx);
                if (s.kind != CHC__TOK_STRING) {
                    chc_type_destroy(t, al);
                    return chc__err_set(err, CHC_ERR_TYPE, "DateTime64: expected tz");
                }
                t->temporal.tz = chc__strdup(al, s.start, s.len, err);
                if (!t->temporal.tz) { chc_type_destroy(t, al); return CHC_ERR_OOM; }
                t->temporal.tz_len = s.len;
            }
        } else if (t->kind == CHC_DATETIME) {
            chc__tok s = chc__eat_tok(lx);
            if (s.kind != CHC__TOK_STRING) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "DateTime: expected tz");
            }
            t->temporal.tz = chc__strdup(al, s.start, s.len, err);
            if (!t->temporal.tz) { chc_type_destroy(t, al); return CHC_ERR_OOM; }
            t->temporal.tz_len = s.len;
        } else if (t->kind == CHC_OBJECT) {
            /* Object('name') -- legacy JSON object syntax. Argument is a
             * schema identifier (eg 'json'); clickhouse-cpp accepts any
             * quoted string. Wire format matches CHC_JSON, so we discard
             * the argument and keep the full source text in t->name for
             * round-trip & error messages. */
            chc__tok s = chc__eat_tok(lx);
            if (s.kind != CHC__TOK_STRING) {
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "Object: expected name");
            }
        } else {
            /* Generic composite: comma-separated type list. Tuple children
             * may carry an optional leading NAME (field label) before the
             * type. Field names are stored in a parallel array on the
             * parent. */
            bool    is_tuple  = (t->kind == CHC_TUPLE);
            char  **fn_buf    = NULL;
            size_t *fn_lens   = NULL;
            size_t  fn_cap    = 0;
            bool    any_named = false;
            for (;;) {
                chc__tok la = chc__peek_tok(lx);
                if (la.kind == CHC__TOK_RPAREN) break;

                chc__tok field = {0};
                bool has_field = false;
                if (is_tuple && la.kind == CHC__TOK_NAME) {
                    chc__eat_tok(lx);
                    if (la.quote) {
                        /* `\`x\`` or `"x"` is never a type head, so it must be
                         * the field label. */
                        field = la;
                        has_field = true;
                    } else {
                        chc__tok la2 = chc__peek_tok(lx);
                        if (la2.kind == CHC__TOK_NAME) {
                            /* `la` is a field-name; `la2` starts the type. */
                            field = la;
                            has_field = true;
                        } else {
                            /* `la` was the type's leading NAME (terminal or
                             * parametric like `Tuple(LowCardinality(...))`).
                             * Put it back & rewind cur to la2's start so the
                             * next peek re-lexes la2. */
                            lx->peeked   = la;
                            lx->has_peek = true;
                            lx->cur      = la2.start;
                        }
                    }
                }

                chc_type *child = NULL;
                int rc = chc__parse_type(lx, al, whole_start, whole_end, depth + 1, &child, err);
                if (rc == CHC_OK)
                    rc = chc__type_push_child(al, t, child, err);
                else
                    child = NULL;
                if (rc != CHC_OK) {
                    if (child) chc_type_destroy(child, al);
                    if (fn_buf) {
                        for (size_t i = 0; i < fn_cap; i++)
                            al->free(al->ud, fn_buf[i], fn_lens[i] + 1);
                        al->free(al->ud, fn_buf,  fn_cap * sizeof *fn_buf);
                        al->free(al->ud, fn_lens, fn_cap * sizeof *fn_lens);
                    }
                    chc_type_destroy(t, al);
                    return rc;
                }

                if (is_tuple) {
                    size_t new_cap = t->n_children;
                    char **nfn = chc__realloc(al, fn_buf,
                                              fn_cap * sizeof *fn_buf,
                                              new_cap * sizeof *fn_buf, err);
                    if (!nfn) { chc_type_destroy(t, al); return CHC_ERR_OOM; }
                    size_t *nfl = chc__realloc(al, fn_lens,
                                               fn_cap * sizeof *fn_lens,
                                               new_cap * sizeof *fn_lens, err);
                    if (!nfl) {
                        al->free(al->ud, nfn, new_cap * sizeof *nfn);
                        chc_type_destroy(t, al); return CHC_ERR_OOM;
                    }
                    fn_buf  = nfn;
                    fn_lens = nfl;
                    fn_buf[fn_cap]  = NULL;
                    fn_lens[fn_cap] = 0;
                    fn_cap = new_cap;
                    if (has_field) {
                        size_t flen = field.len;
                        if (field.quote)
                            fn_buf[fn_cap - 1] = chc__strdup_unquote(al, field.start,
                                                                     field.len, field.quote,
                                                                     &flen, err);
                        else
                            fn_buf[fn_cap - 1] = chc__strdup(al, field.start,
                                                             field.len, err);
                        if (!fn_buf[fn_cap - 1]) {
                            for (size_t i = 0; i < fn_cap - 1; i++)
                                al->free(al->ud, fn_buf[i], fn_lens[i] + 1);
                            al->free(al->ud, fn_buf,  fn_cap * sizeof *fn_buf);
                            al->free(al->ud, fn_lens, fn_cap * sizeof *fn_lens);
                            chc_type_destroy(t, al); return CHC_ERR_OOM;
                        }
                        fn_lens[fn_cap - 1] = flen;
                        any_named = true;
                    }
                }

                chc__tok c = chc__peek_tok(lx);
                if (c.kind == CHC__TOK_COMMA) { chc__eat_tok(lx); continue; }
                if (c.kind == CHC__TOK_RPAREN) break;
                if (fn_buf) {
                    for (size_t i = 0; i < fn_cap; i++)
                        al->free(al->ud, fn_buf[i], fn_lens[i] + 1);
                    al->free(al->ud, fn_buf,  fn_cap * sizeof *fn_buf);
                    al->free(al->ud, fn_lens, fn_cap * sizeof *fn_lens);
                }
                chc_type_destroy(t, al);
                return chc__err_set(err, CHC_ERR_TYPE, "expected ',' or ')'");
            }
            if (any_named) {
                t->field_names     = fn_buf;
                t->field_name_lens = fn_lens;
            } else {
                al->free(al->ud, fn_buf,  fn_cap * sizeof *fn_buf);
                al->free(al->ud, fn_lens, fn_cap * sizeof *fn_lens);
            }
        }

        chc__tok rp = chc__eat_tok(lx);
        if (rp.kind != CHC__TOK_RPAREN) {
            chc_type_destroy(t, al);
            return chc__err_set(err, CHC_ERR_TYPE, "expected ')'");
        }
        name_end = rp.start + 1;
    }

    /* Decimal(P, S) compatibility: width selected by precision. */
    if (t->kind == CHC_DECIMAL128 && head.len == 7
        && memcmp(head.start, "Decimal", 7) == 0 && t->n_children == 0) {
        /* unparenthesised "Decimal" without (P, S) — treat as Decimal128 */
    }

    t->name = chc__strdup(al, name_start, (size_t) (name_end - name_start), err);
    if (!t->name) { chc_type_destroy(t, al); return CHC_ERR_OOM; }
    t->name_len = (size_t) (name_end - name_start);

    *out = t;
    return CHC_OK;
}

int
chc_type_parse(const char *name, size_t name_len,
               const chc_alloc *al, chc_type **out, chc_err *err)
{
    chc__lex lx = { name, name + name_len, {0}, false };
    int rc = chc__parse_type(&lx, al, name, name + name_len, 0, out, err);
    if (rc != CHC_OK) return rc;
    chc__tok tail = chc__eat_tok(&lx);
    if (tail.kind != CHC__TOK_EOS) {
        chc_type_destroy(*out, al);
        *out = NULL;
        return chc__err_set(err, CHC_ERR_TYPE, "trailing tokens in type name");
    }
    return CHC_OK;
}

size_t
chc_type_format(const chc_type *t, char *buf, size_t buf_len)
{
    if (!t) return 0;
    if (t->name && t->name_len) {
        if (buf && buf_len) {
            size_t take = t->name_len < buf_len - 1 ? t->name_len : buf_len - 1;
            memcpy(buf, t->name, take);
            buf[take] = '\0';
        }
        return t->name_len;
    }
    return 0;
}

/* -------- column internals ---------- */

struct chc_column {
    chc_col_kind layout;
    size_t       n_rows;
    union {
        struct { void *data; size_t elem_size; }                              fixed;
        struct { uint8_t *data; uint64_t *offsets; size_t bytes; }            str;
        struct { uint8_t *null_map; chc_column *inner; }                      nullable;
        struct { uint64_t *offsets; chc_column *values; }                     array;
        struct { chc_column **children; size_t arity; }                       tuple;
        struct { int key_size; void *keys; chc_column *dict; size_t dict_n; } lc;
    };
};

chc_col_kind chc_column_layout(const chc_column *c) { return c ? c->layout : (chc_col_kind) 0; }
size_t       chc_column_n_rows(const chc_column *c) { return c ? c->n_rows : 0; }

const void *chc_column_fixed_data(const chc_column *c, size_t *elem_size)
{
    if (!c || c->layout != CHC_COL_FIXED) { if (elem_size) *elem_size = 0; return NULL; }
    if (elem_size) *elem_size = c->fixed.elem_size;
    return c->fixed.data;
}
const uint8_t  *chc_column_string_data(const chc_column *c)
    { return (c && c->layout == CHC_COL_STRING) ? c->str.data : NULL; }
const uint64_t *chc_column_string_offsets(const chc_column *c)
    { return (c && c->layout == CHC_COL_STRING) ? c->str.offsets : NULL; }
const uint8_t    *chc_column_null_map(const chc_column *c)
    { return (c && c->layout == CHC_COL_NULLABLE) ? c->nullable.null_map : NULL; }
const chc_column *chc_column_nullable_inner(const chc_column *c)
    { return (c && c->layout == CHC_COL_NULLABLE) ? c->nullable.inner : NULL; }
const uint64_t   *chc_column_array_offsets(const chc_column *c)
    { return (c && c->layout == CHC_COL_ARRAY) ? c->array.offsets : NULL; }
const chc_column *chc_column_array_values(const chc_column *c)
    { return (c && c->layout == CHC_COL_ARRAY) ? c->array.values : NULL; }
size_t            chc_column_tuple_arity(const chc_column *c)
    { return (c && c->layout == CHC_COL_TUPLE) ? c->tuple.arity : 0; }
const chc_column *chc_column_tuple_child(const chc_column *c, size_t i)
    { return (c && c->layout == CHC_COL_TUPLE && i < c->tuple.arity) ? c->tuple.children[i] : NULL; }
int               chc_column_lc_key_size(const chc_column *c)
    { return (c && c->layout == CHC_COL_LOW_CARDINALITY) ? c->lc.key_size : 0; }
const void       *chc_column_lc_keys(const chc_column *c)
    { return (c && c->layout == CHC_COL_LOW_CARDINALITY) ? c->lc.keys : NULL; }
const chc_column *chc_column_lc_dict(const chc_column *c)
    { return (c && c->layout == CHC_COL_LOW_CARDINALITY) ? c->lc.dict : NULL; }

static void chc__column_destroy(chc_column *c, const chc_alloc *al);

static void
chc__column_destroy(chc_column *c, const chc_alloc *al)
{
    if (!c) return;
    switch (c->layout) {
    case CHC_COL_FIXED:
        al->free(al->ud, c->fixed.data, c->n_rows * c->fixed.elem_size);
        break;
    case CHC_COL_STRING:
        al->free(al->ud, c->str.data, c->str.bytes);
        al->free(al->ud, c->str.offsets, c->n_rows * sizeof(uint64_t));
        break;
    case CHC_COL_NULLABLE:
        al->free(al->ud, c->nullable.null_map, c->n_rows);
        chc__column_destroy(c->nullable.inner, al);
        break;
    case CHC_COL_ARRAY:
        al->free(al->ud, c->array.offsets, c->n_rows * sizeof(uint64_t));
        chc__column_destroy(c->array.values, al);
        break;
    case CHC_COL_TUPLE:
        for (size_t i = 0; i < c->tuple.arity; i++)
            chc__column_destroy(c->tuple.children[i], al);
        al->free(al->ud, c->tuple.children, c->tuple.arity * sizeof *c->tuple.children);
        break;
    case CHC_COL_LOW_CARDINALITY:
        al->free(al->ud, c->lc.keys, c->n_rows * c->lc.key_size);
        chc__column_destroy(c->lc.dict, al);
        break;
    case CHC_COL_NOTHING:
        break;
    }
    al->free(al->ud, c, sizeof *c);
}

int
chc_column_validate(const chc_column *c, chc_err *err)
{
    if (!c) return CHC_OK;
    switch (c->layout) {
    case CHC_COL_ARRAY: {
        const uint64_t *offs = c->array.offsets;
        uint64_t prev = 0;
        for (size_t i = 0; i < c->n_rows; i++) {
            if (offs[i] < prev)
                return chc__err_set(err, CHC_ERR_PROTOCOL,
                    "array offsets not monotonic at row %zu: %llu < %llu",
                    i, (unsigned long long) offs[i], (unsigned long long) prev);
            prev = offs[i];
        }
        return chc_column_validate(c->array.values, err);
    }
    case CHC_COL_LOW_CARDINALITY: {
        size_t dn = c->lc.dict_n;
        const void *k = c->lc.keys;
        for (size_t i = 0; i < c->n_rows; i++) {
            uint64_t v;
            switch (c->lc.key_size) {
            case 1: v = ((const uint8_t  *) k)[i]; break;
            case 2: v = ((const uint16_t *) k)[i]; break;
            case 4: v = ((const uint32_t *) k)[i]; break;
            case 8: v = ((const uint64_t *) k)[i]; break;
            default:
                return chc__err_set(err, CHC_ERR_PROTOCOL,
                    "LowCardinality: invalid key_size %d", c->lc.key_size);
            }
            if (v >= dn)
                return chc__err_set(err, CHC_ERR_PROTOCOL,
                    "LowCardinality key out of range at row %zu: %llu >= dict_n %zu",
                    i, (unsigned long long) v, dn);
        }
        return chc_column_validate(c->lc.dict, err);
    }
    case CHC_COL_NULLABLE:
        return chc_column_validate(c->nullable.inner, err);
    case CHC_COL_TUPLE:
        for (size_t i = 0; i < c->tuple.arity; i++) {
            int rc = chc_column_validate(c->tuple.children[i], err);
            if (rc != CHC_OK) return rc;
        }
        return CHC_OK;
    default:
        return CHC_OK;
    }
}

/* -------- column reader (recursive on type kind) ---------- */

/* Elem-size table for FIXED kinds. Returns 0 if `t` isn't a FIXED kind. */
size_t
chc_type_elem_size(const chc_type *t)
{
    switch (t->kind) {
    case CHC_INT8:  case CHC_UINT8:  case CHC_BOOL:                 return 1;
    case CHC_INT16: case CHC_UINT16: case CHC_DATE:
    case CHC_ENUM16: case CHC_BFLOAT16:                             return 2;
    case CHC_INT32: case CHC_UINT32: case CHC_DATE32:
    case CHC_DATETIME: case CHC_FLOAT32: case CHC_DECIMAL32:
    case CHC_TIME: case CHC_IPV4:                                   return 4;
    case CHC_INT64: case CHC_UINT64: case CHC_DATETIME64:
    case CHC_FLOAT64: case CHC_DECIMAL64: case CHC_TIME64:
    case CHC_INTERVAL:                                              return 8;
    case CHC_INT128: case CHC_UINT128: case CHC_DECIMAL128:
    case CHC_UUID: case CHC_IPV6:                                   return 16;
    case CHC_INT256: case CHC_UINT256: case CHC_DECIMAL256:         return 32;
    case CHC_ENUM8:                                                 return 1;
    case CHC_FIXED_STRING:                                          return (size_t) t->fixed_string.n;
    default: return 0;
    }
}

/* LowCardinality on-wire flag word constants. */
#define CHC__LC_INDEX_TYPE_MASK     0xffu
#define CHC__LC_NEED_GLOBAL_DICT    (1u << 8)
#define CHC__LC_HAS_ADDITIONAL_KEYS (1u << 9)
#define CHC__LC_NEED_UPDATE_DICT    (1u << 10)

static int chc__col_read(chc_in *in, const chc_type *t,
                         size_t n_rows, chc_column **out, chc_err *err);

static int
chc__col_read_fixed(chc_in *in, size_t elem_size, size_t n_rows,
                    chc_column **out, chc_err *err)
{
    const chc_alloc *al = in->al;
    chc_column *c = chc__calloc(al, sizeof *c, err);
    if (!c) return CHC_ERR_OOM;
    c->layout = CHC_COL_FIXED;
    c->n_rows = n_rows;
    c->fixed.elem_size = elem_size;
    if (n_rows && elem_size) {
        size_t nbytes;
        if (chc__mul_size(n_rows, elem_size, &nbytes)) {
            chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL, "column size overflow");
        }
        c->fixed.data = chc__alloc(al, nbytes, err);
        if (!c->fixed.data) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
        int rc = chc__read_bytes(in, c->fixed.data, nbytes, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
    }
    *out = c;
    return CHC_OK;
}

static int
chc__col_read_string(chc_in *in, size_t n_rows,
                     chc_column **out, chc_err *err)
{
    const chc_alloc *al = in->al;
    chc_column *c = chc__calloc(al, sizeof *c, err);
    if (!c) return CHC_ERR_OOM;
    c->layout = CHC_COL_STRING;
    c->n_rows = n_rows;
    if (!n_rows) { *out = c; return CHC_OK; }
    size_t offs_bytes;
    if (chc__mul_size(n_rows, sizeof(uint64_t), &offs_bytes)) {
        chc__column_destroy(c, al);
        return chc__err_set(err, CHC_ERR_PROTOCOL, "string column size overflow");
    }
    c->str.offsets = chc__alloc(al, offs_bytes, err);
    if (!c->str.offsets) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
    size_t cap = 256;
    size_t total = 0;
    c->str.data = chc__alloc(al, cap, err);
    if (!c->str.data) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
    c->str.bytes = cap;
    for (size_t i = 0; i < n_rows; i++) {
        uint64_t len;
        int rc = chc__read_varuint(in, &len, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        if (len > CHC_MAX_STRING_SIZE) { chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL, "string row too long"); }
        if (total + len > cap) {
            size_t new_cap = cap;
            while (new_cap < total + len) new_cap *= 2;
            uint8_t *r = chc__realloc(al, c->str.data, cap, new_cap, err);
            if (!r) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
            c->str.data = r;
            cap = new_cap;
            c->str.bytes = cap;
        }
        if (len) {
            rc = chc__read_bytes(in, c->str.data + total, (size_t) len, err);
            if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        }
        total += len;
        c->str.offsets[i] = total;
    }
    *out = c;
    return CHC_OK;
}

/* Composite columns might have a prefix sub-stream. Only LowCardinality
 * actually emits one in the formats we handle: a uint64 key version. */
static int
chc__col_read_prefix(chc_in *in, const chc_type *t, chc_err *err)
{
    if (t->kind == CHC_LOW_CARDINALITY) {
        uint64_t v;
        int rc = chc__read_u64_le(in, &v, err);
        if (rc != CHC_OK) return rc;
        if (v != 1)
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "LowCardinality: unexpected key version %llu", (unsigned long long) v);
        return CHC_OK;
    }
    if (t->kind == CHC_NULLABLE || t->kind == CHC_ARRAY
        || t->kind == CHC_TUPLE || t->kind == CHC_MAP
        || t->kind == CHC_SIMPLE_AGGREGATE_FUNCTION) {
        for (size_t i = 0; i < t->n_children; i++) {
            int rc = chc__col_read_prefix(in, t->children[i], err);
            if (rc != CHC_OK) return rc;
        }
    }
    return CHC_OK;
}

/* Geo types are aliases for nested Array(...(Tuple(Float64,Float64))). depth
 * 0 = Point, 1 = Ring (Array(Point)), 2 = Polygon (Array(Ring)),
 * 3 = MultiPolygon (Array(Polygon)). Defined ahead of chc__col_read so it
 * can call back into here. */
static int chc__col_read_geo(chc_in *in, int depth, size_t n_rows,
                             chc_column **out, chc_err *err);

/* Byte-swap a host-typed uint64/keys array in place on BE hosts. No-op on LE. */
static void
chc__swap_offsets(CHC_MAYBE_UNUSED uint64_t *p, CHC_MAYBE_UNUSED size_t n)
{
#if CHC_BIG_ENDIAN
    for (size_t i = 0; i < n; i++) p[i] = chc__bswap64(p[i]);
#endif
}

static void
chc__swap_keys(CHC_MAYBE_UNUSED void *p, CHC_MAYBE_UNUSED size_t n,
               CHC_MAYBE_UNUSED int key_size)
{
#if CHC_BIG_ENDIAN
    switch (key_size) {
    case 1: break;
    case 2: { uint16_t *a = p; for (size_t i = 0; i < n; i++) a[i] = chc__bswap16(a[i]); break; }
    case 4: { uint32_t *a = p; for (size_t i = 0; i < n; i++) a[i] = chc__bswap32(a[i]); break; }
    case 8: { uint64_t *a = p; for (size_t i = 0; i < n; i++) a[i] = chc__bswap64(a[i]); break; }
    }
#endif
}

static int
chc__col_read(chc_in *in, const chc_type *t,
              size_t n_rows, chc_column **out, chc_err *err)
{
    const chc_alloc *al = in->al;
    /* Tier 1 / FIXED scalar */
    size_t es = chc_type_elem_size(t);
    if (es) return chc__col_read_fixed(in, es, n_rows, out, err);

    switch (t->kind) {
    case CHC_STRING:
        return chc__col_read_string(in, n_rows, out, err);

    case CHC_JSON:
    case CHC_OBJECT: {
        /* JSON / Object('json') stream prefix: 8-byte LE serialization
         * version (SerializationObject.cpp:275). Only STRING (=1) is in
         * scope; other versions need the consumer to set
         * output_format_native_write_json_as_string=1 on the SELECT.
         * Body bytes per row are writeStringBinary, identical to a String
         * column — reuse chc__col_read_string and keep CHC_COL_STRING
         * layout so callers reuse string accessors. */
        uint64_t version;
        int rc = chc__read_u64_le(in, &version, err);
        if (rc != CHC_OK) return rc;
        if (version != 1)
            return chc__err_set(err, CHC_ERR_TYPE,
                "unsupported JSON serialization version %llu "
                "(set output_format_native_write_json_as_string=1)",
                (unsigned long long) version);
        return chc__col_read_string(in, n_rows, out, err);
    }

    case CHC_NULLABLE: {
        if (t->n_children != 1)
            return chc__err_set(err, CHC_ERR_TYPE, "Nullable expects 1 child");
        chc_column *c = chc__calloc(al, sizeof *c, err);
        if (!c) return CHC_ERR_OOM;
        c->layout = CHC_COL_NULLABLE;
        c->n_rows = n_rows;
        if (n_rows) {
            c->nullable.null_map = chc__alloc(al, n_rows, err);
            if (!c->nullable.null_map) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
            int rc = chc__read_bytes(in, c->nullable.null_map, n_rows, err);
            if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        }
        int rc = chc__col_read(in, t->children[0], n_rows, &c->nullable.inner, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        *out = c;
        return CHC_OK;
    }

    case CHC_ARRAY:
    case CHC_MAP: {
        if (t->kind == CHC_ARRAY && t->n_children != 1)
            return chc__err_set(err, CHC_ERR_TYPE, "Array expects 1 child");
        if (t->kind == CHC_MAP && t->n_children != 2)
            return chc__err_set(err, CHC_ERR_TYPE, "Map expects 2 children");
        chc_column *c = chc__calloc(al, sizeof *c, err);
        if (!c) return CHC_ERR_OOM;
        c->layout = CHC_COL_ARRAY;
        c->n_rows = n_rows;
        uint64_t total = 0;
        if (n_rows) {
            c->array.offsets = chc__alloc(al, n_rows * sizeof(uint64_t), err);
            if (!c->array.offsets) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
            int rc = chc__read_bytes(in, c->array.offsets,
                                     n_rows * sizeof(uint64_t), err);
            if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
            chc__swap_offsets(c->array.offsets, n_rows);
            total = c->array.offsets[n_rows - 1];
            if (total > CHC_MAX_NUM_ROWS) {
                chc__column_destroy(c, al);
                return chc__err_set(err, CHC_ERR_PROTOCOL,
                    "array nested length too large: %llu",
                    (unsigned long long) total);
            }
        }
        if (t->kind == CHC_ARRAY) {
            int rc = chc__col_read(in, t->children[0], (size_t) total,
                                   &c->array.values, err);
            if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        } else {
            /* Map: synthesise an implicit Tuple(K, V) column. */
            chc_column *tup = chc__calloc(al, sizeof *tup, err);
            if (!tup) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
            tup->layout = CHC_COL_TUPLE;
            tup->n_rows = (size_t) total;
            tup->tuple.arity = 2;
            tup->tuple.children = chc__calloc(al, 2 * sizeof *tup->tuple.children, err);
            if (!tup->tuple.children) { chc__column_destroy(tup, al); chc__column_destroy(c, al); return CHC_ERR_OOM; }
            int rc = chc__col_read(in, t->children[0], (size_t) total,
                                   &tup->tuple.children[0], err);
            if (rc != CHC_OK) { chc__column_destroy(tup, al); chc__column_destroy(c, al); return rc; }
            rc = chc__col_read(in, t->children[1], (size_t) total,
                               &tup->tuple.children[1], err);
            if (rc != CHC_OK) { chc__column_destroy(tup, al); chc__column_destroy(c, al); return rc; }
            c->array.values = tup;
        }
        *out = c;
        return CHC_OK;
    }

    case CHC_TUPLE: {
        chc_column *c = chc__calloc(al, sizeof *c, err);
        if (!c) return CHC_ERR_OOM;
        c->layout = CHC_COL_TUPLE;
        c->n_rows = n_rows;
        c->tuple.arity = t->n_children;
        c->tuple.children = chc__calloc(al, t->n_children * sizeof *c->tuple.children, err);
        if (!c->tuple.children) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
        for (size_t i = 0; i < t->n_children; i++) {
            int rc = chc__col_read(in, t->children[i], n_rows,
                                   &c->tuple.children[i], err);
            if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        }
        *out = c;
        return CHC_OK;
    }

    case CHC_LOW_CARDINALITY: {
        if (t->n_children != 1)
            return chc__err_set(err, CHC_ERR_TYPE, "LowCardinality expects 1 child");
        const chc_type *inner = t->children[0];
        const chc_type *dict_type = inner;
        bool nullable_wrap = false;
        if (inner->kind == CHC_NULLABLE) {
            nullable_wrap = true;
            dict_type = inner->children[0];
        }

        chc_column *c = chc__calloc(al, sizeof *c, err);
        if (!c) return CHC_ERR_OOM;
        c->layout = CHC_COL_LOW_CARDINALITY;
        c->n_rows = n_rows;

        if (n_rows == 0) {
            /* Empty LC column has no body at all. */
            chc_column *empty_dict = chc__calloc(al, sizeof *empty_dict, err);
            if (!empty_dict) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
            empty_dict->layout = (dict_type->kind == CHC_STRING) ? CHC_COL_STRING
                              : (chc_type_elem_size(dict_type) ? CHC_COL_FIXED : CHC_COL_NOTHING);
            if (empty_dict->layout == CHC_COL_FIXED)
                empty_dict->fixed.elem_size = chc_type_elem_size(dict_type);
            c->lc.dict = empty_dict;
            c->lc.key_size = 1;
            *out = c;
            return CHC_OK;
        }

        uint64_t flags;
        int rc = chc__read_u64_le(in, &flags, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        if (flags & CHC__LC_NEED_GLOBAL_DICT) {
            chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "LowCardinality: global dictionary not supported");
        }
        if (!(flags & CHC__LC_HAS_ADDITIONAL_KEYS)) {
            chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "LowCardinality: HasAdditionalKeys missing");
        }
        unsigned idx_type = (unsigned) (flags & CHC__LC_INDEX_TYPE_MASK);
        switch (idx_type) {
        case 0: c->lc.key_size = 1; break;
        case 1: c->lc.key_size = 2; break;
        case 2: c->lc.key_size = 4; break;
        case 3: c->lc.key_size = 8; break;
        default: chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "LowCardinality: invalid index type %u", idx_type);
        }

        uint64_t dict_n;
        rc = chc__read_u64_le(in, &dict_n, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        if (dict_n > CHC_MAX_NUM_ROWS) {
            chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "LowCardinality dictionary too large: %llu",
                (unsigned long long) dict_n);
        }
        chc_column *inner_dict = NULL;
        rc = chc__col_read(in, dict_type, (size_t) dict_n, &inner_dict, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        c->lc.dict_n = (size_t) dict_n;

        if (nullable_wrap) {
            /* Wire convention: slot 0 of the inner-typed dict is the NULL
             * sentinel (clickhouse-cpp/columns/lowcardinality.cpp 287-295).
             * Wrap the dict in a Nullable column so the caller's standard
             * null-map dispatch covers the LC(Nullable) case. */
            chc_column *wrapped = chc__calloc(al, sizeof *wrapped, err);
            if (!wrapped) { chc__column_destroy(inner_dict, al); chc__column_destroy(c, al); return CHC_ERR_OOM; }
            wrapped->layout = CHC_COL_NULLABLE;
            wrapped->n_rows = (size_t) dict_n;
            wrapped->nullable.inner = inner_dict;
            if (dict_n) {
                wrapped->nullable.null_map = chc__calloc(al, (size_t) dict_n, err);
                if (!wrapped->nullable.null_map) {
                    chc__column_destroy(wrapped, al); chc__column_destroy(c, al);
                    return CHC_ERR_OOM;
                }
                wrapped->nullable.null_map[0] = 1;
            }
            c->lc.dict = wrapped;
        } else {
            c->lc.dict = inner_dict;
        }

        uint64_t key_rows;
        rc = chc__read_u64_le(in, &key_rows, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        if (key_rows != n_rows) {
            chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "LowCardinality: key_rows %llu != block rows %zu",
                (unsigned long long) key_rows, n_rows);
        }
        c->lc.keys = chc__alloc(al, n_rows * c->lc.key_size, err);
        if (!c->lc.keys) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
        rc = chc__read_bytes(in, c->lc.keys, n_rows * c->lc.key_size, err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        chc__swap_keys(c->lc.keys, n_rows, c->lc.key_size);

        *out = c;
        return CHC_OK;
    }

    case CHC_SIMPLE_AGGREGATE_FUNCTION:
        /* Wire form is the inner type's stream. Last child is the data type. */
        if (t->n_children < 1)
            return chc__err_set(err, CHC_ERR_TYPE, "SimpleAggregateFunction has no inner type");
        return chc__col_read(in, t->children[t->n_children - 1], n_rows, out, err);

    /* Geo types: aliases for nested Array layers terminating in
     * Tuple(Float64, Float64). Per clickhouse-cpp factory.cpp 120-130. */
    case CHC_POINT:          return chc__col_read_geo(in, 0, n_rows, out, err);
    case CHC_RING:           return chc__col_read_geo(in, 1, n_rows, out, err);
    case CHC_POLYGON:        return chc__col_read_geo(in, 2, n_rows, out, err);
    case CHC_MULTI_POLYGON:  return chc__col_read_geo(in, 3, n_rows, out, err);

    case CHC_NOTHING:
    case CHC_VOID: {
        chc_column *c = chc__calloc(al, sizeof *c, err);
        if (!c) return CHC_ERR_OOM;
        c->layout = CHC_COL_NOTHING;
        c->n_rows = n_rows;
        /* Wire shape for Nothing is a sequence of UInt8 bytes per row. */
        if (n_rows) {
            uint8_t throwaway[256];
            size_t left = n_rows;
            while (left) {
                size_t take = left < sizeof throwaway ? left : sizeof throwaway;
                int rc = chc__read_bytes(in, throwaway, take, err);
                if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
                left -= take;
            }
        }
        *out = c;
        return CHC_OK;
    }

    default: {
        size_t nl;
        const char *nm = chc_type_name(t, &nl);
        return chc__err_set(err, CHC_ERR_TYPE,
            "unsupported column type: %.*s", (int) nl, nm ? nm : "");
    }
    }
}

static int
chc__col_read_geo(chc_in *in, int depth, size_t n_rows,
                  chc_column **out, chc_err *err)
{
    const chc_alloc *al = in->al;
    if (depth == 0) {
        /* Point = Tuple(Float64, Float64). */
        chc_column *c = chc__calloc(al, sizeof *c, err);
        if (!c) return CHC_ERR_OOM;
        c->layout = CHC_COL_TUPLE;
        c->n_rows = n_rows;
        c->tuple.arity = 2;
        c->tuple.children = chc__calloc(al, 2 * sizeof *c->tuple.children, err);
        if (!c->tuple.children) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
        for (int i = 0; i < 2; i++) {
            int rc = chc__col_read_fixed(in, 8, n_rows, &c->tuple.children[i], err);
            if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        }
        *out = c;
        return CHC_OK;
    }
    /* Array(geo(depth-1)). */
    chc_column *c = chc__calloc(al, sizeof *c, err);
    if (!c) return CHC_ERR_OOM;
    c->layout = CHC_COL_ARRAY;
    c->n_rows = n_rows;
    uint64_t total = 0;
    if (n_rows) {
        c->array.offsets = chc__alloc(al, n_rows * sizeof(uint64_t), err);
        if (!c->array.offsets) { chc__column_destroy(c, al); return CHC_ERR_OOM; }
        int rc = chc__read_bytes(in, c->array.offsets,
                                 n_rows * sizeof(uint64_t), err);
        if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
        chc__swap_offsets(c->array.offsets, n_rows);
        total = c->array.offsets[n_rows - 1];
        if (total > CHC_MAX_NUM_ROWS) {
            chc__column_destroy(c, al);
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "array nested length too large: %llu",
                (unsigned long long) total);
        }
    }
    int rc = chc__col_read_geo(in, depth - 1, (size_t) total,
                               &c->array.values, err);
    if (rc != CHC_OK) { chc__column_destroy(c, al); return rc; }
    *out = c;
    return CHC_OK;
}

/* -------- block reader ---------- */

struct chc_block {
    size_t        n_columns;
    size_t        n_rows;
    bool          is_overflows;
    int32_t       bucket_num;
    char        **names;
    size_t       *name_lens;
    chc_type    **types;
    chc_column  **columns;
};

void
chc_block_destroy(chc_block *b, const chc_alloc *al)
{
    if (!b) return;
    for (size_t i = 0; i < b->n_columns; i++) {
        if (b->names && b->names[i])
            al->free(al->ud, b->names[i], b->name_lens[i] + 1);
        if (b->types) chc_type_destroy(b->types[i], al);
        if (b->columns) chc__column_destroy(b->columns[i], al);
    }
    al->free(al->ud, b->names,     b->n_columns * sizeof *b->names);
    al->free(al->ud, b->name_lens, b->n_columns * sizeof *b->name_lens);
    al->free(al->ud, b->types,     b->n_columns * sizeof *b->types);
    al->free(al->ud, b->columns,   b->n_columns * sizeof *b->columns);
    al->free(al->ud, b, sizeof *b);
}

size_t            chc_block_n_rows(const chc_block *b)    { return b ? b->n_rows : 0; }
size_t            chc_block_n_columns(const chc_block *b) { return b ? b->n_columns : 0; }
const char       *chc_block_column_name(const chc_block *b, size_t i, size_t *out_len) {
    if (!b || i >= b->n_columns) { if (out_len) *out_len = 0; return NULL; }
    if (out_len) *out_len = b->name_lens[i];
    return b->names[i];
}
const chc_type   *chc_block_column_type(const chc_block *b, size_t i)
    { return (b && i < b->n_columns) ? b->types[i] : NULL; }
const chc_column *chc_block_column(const chc_block *b, size_t i)
    { return (b && i < b->n_columns) ? b->columns[i] : NULL; }
bool              chc_block_is_overflows(const chc_block *b) { return b ? b->is_overflows : false; }
int32_t           chc_block_bucket_num(const chc_block *b)   { return b ? b->bucket_num : 0; }

/* Block read using an already-initialised chc_in. Used by chc_block_read
 * (one-shot) and by clickhouse-client.h's recv_packet (persistent buffer).
 * Returns 0 with *out == NULL on clean EOF at block boundary (only when
 * opts->has_block_info is false; TCP path has no clean-EOF concept). */
static int
chc__block_read_in(chc_in *in, const chc_alloc *al,
                   const chc_block_opts *opts, chc_block **out, chc_err *err)
{
    chc_block *b = chc__calloc(al, sizeof *b, err);
    if (!b) return CHC_ERR_OOM;
    int rc;
    uint64_t consumed_before = in->consumed;

    if (opts->has_block_info) {
        uint64_t fid;
        rc = chc__read_varuint(in, &fid, err);
        if (rc == CHC_ERR_EOF && in->consumed == consumed_before) {
            chc_block_destroy(b, al);
            *out = NULL;
            chc_err_reset(err);
            return CHC_OK;
        }
        if (rc != CHC_OK) goto fail;
        if (fid != 1) { rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                            "BlockInfo: expected field 1, got %llu",
                            (unsigned long long) fid); goto fail; }
        uint8_t ov;
        rc = chc__read_byte(in, &ov, err); if (rc != CHC_OK) goto fail;
        b->is_overflows = ov != 0;
        rc = chc__read_varuint(in, &fid, err); if (rc != CHC_OK) goto fail;
        if (fid != 2) { rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                            "BlockInfo: expected field 2"); goto fail; }
        uint32_t bn;
        rc = chc__read_u32_le(in, &bn, err); if (rc != CHC_OK) goto fail;
        b->bucket_num = (int32_t) bn;
        rc = chc__read_varuint(in, &fid, err); if (rc != CHC_OK) goto fail;
        if (fid != 0) { rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                            "BlockInfo: expected terminator"); goto fail; }
    }

    uint64_t ncols, nrows;
    rc = chc__read_varuint(in, &ncols, err);
    if (rc == CHC_ERR_EOF && !opts->has_block_info
        && in->consumed == consumed_before) {
        chc_block_destroy(b, al);
        *out = NULL;
        chc_err_reset(err);
        return CHC_OK;
    }
    if (rc != CHC_OK) goto fail;
    rc = chc__read_varuint(in, &nrows, err);
    if (rc != CHC_OK) goto fail;

    if (ncols > CHC_MAX_NUM_COLUMNS) {
        rc = chc__err_set(err, CHC_ERR_PROTOCOL,
            "suspiciously many columns: %llu", (unsigned long long) ncols);
        goto fail;
    }
    if (nrows > CHC_MAX_NUM_ROWS) {
        rc = chc__err_set(err, CHC_ERR_PROTOCOL,
            "suspiciously many rows: %llu", (unsigned long long) nrows);
        goto fail;
    }

    b->n_columns = (size_t) ncols;
    b->n_rows    = (size_t) nrows;
    if (ncols) {
        b->names     = chc__calloc(al, ncols * sizeof *b->names,     err);
        b->name_lens = chc__calloc(al, ncols * sizeof *b->name_lens, err);
        b->types     = chc__calloc(al, ncols * sizeof *b->types,     err);
        b->columns   = chc__calloc(al, ncols * sizeof *b->columns,   err);
        if (!b->names || !b->name_lens || !b->types || !b->columns) {
            rc = CHC_ERR_OOM; goto fail;
        }
    }

    for (size_t i = 0; i < (size_t) ncols; i++) {
        rc = chc__read_string(in, &b->names[i], &b->name_lens[i], err);
        if (rc != CHC_OK) goto fail;

        char *type_name; size_t type_len;
        rc = chc__read_string(in, &type_name, &type_len, err);
        if (rc != CHC_OK) goto fail;

        if (opts->has_custom_serialization) {
            uint8_t hcs;
            rc = chc__read_byte(in, &hcs, err);
            if (rc != CHC_OK) { al->free(al->ud, type_name, type_len + 1); goto fail; }
            if (hcs) {
                rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                    "custom serialization not supported on column '%s'", b->names[i]);
                al->free(al->ud, type_name, type_len + 1);
                goto fail;
            }
        }

        rc = chc_type_parse(type_name, type_len, al, &b->types[i], err);
        al->free(al->ud, type_name, type_len + 1);
        if (rc != CHC_OK) goto fail;

        if (nrows) {
            rc = chc__col_read_prefix(in, b->types[i], err);
            if (rc != CHC_OK) goto fail;

            rc = chc__col_read(in, b->types[i], (size_t) nrows,
                               &b->columns[i], err);
            if (rc != CHC_OK) goto fail;
        }
    }

    *out = b;
    return CHC_OK;

fail:
    chc_block_destroy(b, al);
    *out = NULL;
    return rc;
}

int
chc_block_read(chc_io *io, const chc_alloc *al,
               const chc_block_opts *opts,
               chc_block **out, chc_err *err)
{
    chc_block_opts def = {0};
    if (!opts) opts = &def;
    chc_in in;
    int rc = chc_in_init(&in, io, al, opts->read_buffer_bytes, err);
    if (rc != CHC_OK) return rc;
    rc = chc__block_read_in(&in, al, opts, out, err);
    chc_in_free(&in);
    return rc;
}

/* -------- block writer ---------- */

typedef enum {
    CHC__BLD_FIXED               = 1,
    CHC__BLD_STRING              = 2,
    CHC__BLD_NULL_FIXED          = 3,
    CHC__BLD_NULL_STRING         = 4,
    CHC__BLD_ARRAY_FIXED         = 5,
    CHC__BLD_ARRAY_STRING        = 6,
    CHC__BLD_LC_STRING           = 7,
    CHC__BLD_JSON_STRING         = 8,
    CHC__BLD_ARRAY_NESTED_FIXED  = 9,
    CHC__BLD_ARRAY_NESTED_STRING = 10,
} chc__bld_kind;

typedef struct {
    const char     *name;
    size_t          name_len;
    const chc_type *type;             /* NULL only for legacy STRING entries */
    chc__bld_kind   kind;
    size_t          n_rows;
    size_t          inner_n;          /* element count of the inner array/string/dict body */
    /* Pointers into caller-owned memory; library never copies. */
    /* Base representation: fixed-width xor variable-length. */
    union {
        struct { const void *data; size_t elem_size; }           fixed;  /* *_FIXED */
        struct { const uint64_t *offsets; const uint8_t *data; } str;    /* *_STRING / LC dict */
    };
    /* Structural modifier over base; absent for plain FIXED / STRING / JSON. */
    union {
        struct { const uint8_t *null_map; }    nullable;  /* NULL_* */
        struct { const uint64_t *offsets; }    array;     /* ARRAY_FIXED / ARRAY_STRING (cumulative ends) */
        struct {                                          /* ARRAY_NESTED_*, ndim >= 2 */
            int                     ndim;
            const uint64_t * const *level_offsets;        /* ndim cumulative-end arrays */
            const size_t           *level_offsets_len;    /* count per level */
        } nested;
        struct { int key_size; const void *keys; } lc;    /* LC_STRING */
    };
} chc__col_entry;

struct chc_block_builder {
    const chc_alloc *al;          /* captured at init */
    chc__col_entry  *cols;
    size_t           n_cols;
    size_t           cap;
    size_t           n_rows;      /* common across all columns */
    bool             n_rows_set;
};

int
chc_block_builder_init(chc_block_builder **out, const chc_alloc *al,
                       chc_err *err)
{
    chc_block_builder *bb = chc__calloc(al, sizeof *bb, err);
    if (!bb) return CHC_ERR_OOM;
    bb->al = al;
    *out = bb;
    return CHC_OK;
}

void
chc_block_builder_destroy(chc_block_builder *bb)
{
    if (!bb) return;
    const chc_alloc *al = bb->al;
    al->free(al->ud, bb->cols, bb->cap * sizeof *bb->cols);
    al->free(al->ud, bb, sizeof *bb);
}

static int
chc__bld_grow(chc_block_builder *bb, chc_err *err)
{
    if (bb->n_cols < bb->cap) return CHC_OK;
    size_t new_cap = bb->cap ? bb->cap * 2 : 4;
    chc__col_entry *p = chc__realloc(bb->al, bb->cols,
                                     bb->cap * sizeof *bb->cols,
                                     new_cap * sizeof *bb->cols, err);
    if (!p) return CHC_ERR_OOM;
    bb->cols = p;
    bb->cap = new_cap;
    return CHC_OK;
}

static int
chc__bld_check_rows(chc_block_builder *bb, size_t n_rows, chc_err *err)
{
    if (!bb->n_rows_set) { bb->n_rows = n_rows; bb->n_rows_set = true; return CHC_OK; }
    if (bb->n_rows != n_rows)
        return chc__err_set(err, CHC_ERR_USAGE,
            "block_builder: row count mismatch (%zu vs %zu)", bb->n_rows, n_rows);
    return CHC_OK;
}

int
chc_block_builder_append_fixed(chc_block_builder *bb,
                               const char *name, size_t name_len,
                               const chc_type *t,
                               const void *data, size_t n_rows,
                               chc_err *err)
{
    size_t es = chc_type_elem_size(t);
    if (!es) return chc__err_set(err, CHC_ERR_TYPE,
        "append_fixed: type is not fixed-size");
    int rc = chc__bld_check_rows(bb, n_rows, err);
    if (rc != CHC_OK) return rc;
    rc = chc__bld_grow(bb, err);
    if (rc != CHC_OK) return rc;
    chc__col_entry *e = &bb->cols[bb->n_cols++];
    memset(e, 0, sizeof *e);
    e->name = name; e->name_len = name_len;
    e->type = t;
    e->kind = CHC__BLD_FIXED;
    e->n_rows = n_rows;
    e->fixed.data = data;
    e->fixed.elem_size = es;
    return CHC_OK;
}

int
chc_block_builder_append_string(chc_block_builder *bb,
                                const char *name, size_t name_len,
                                const uint64_t *offsets,
                                const uint8_t *data, size_t n_rows,
                                chc_err *err)
{
    int rc = chc__bld_check_rows(bb, n_rows, err);
    if (rc != CHC_OK) return rc;
    rc = chc__bld_grow(bb, err);
    if (rc != CHC_OK) return rc;
    chc__col_entry *e = &bb->cols[bb->n_cols++];
    memset(e, 0, sizeof *e);
    e->name = name; e->name_len = name_len;
    e->kind = CHC__BLD_STRING;
    e->n_rows = n_rows;
    e->str.offsets = offsets;
    e->str.data = data;
    e->inner_n = n_rows;
    return CHC_OK;
}

/* Extract the inner fixed-elem size from a Nullable(<fixed>) /
 * Array(<fixed>) type. 0 if `t` is not the expected shape. */
static size_t
chc__bld_inner_fixed_size(const chc_type *t, chc_kind outer)
{
    if (!t || t->kind != outer || t->n_children != 1) return 0;
    return chc_type_elem_size(t->children[0]);
}

/* True iff `t` is Array(String) / Nullable(String). */
static bool
chc__bld_inner_is_string(const chc_type *t, chc_kind outer)
{
    return t && t->kind == outer && t->n_children == 1
        && t->children[0]->kind == CHC_STRING;
}

/* True iff `t` is LowCardinality(String) or LowCardinality(Nullable(String)). */
static bool
chc__bld_lc_inner_is_string(const chc_type *t)
{
    if (!t || t->kind != CHC_LOW_CARDINALITY || t->n_children != 1) return false;
    const chc_type *inner = t->children[0];
    if (inner->kind == CHC_STRING) return true;
    if (inner->kind == CHC_NULLABLE && inner->n_children == 1
        && inner->children[0]->kind == CHC_STRING)
        return true;
    return false;
}

int
chc_block_builder_append_nullable_fixed(chc_block_builder *bb,
                                        const char *name, size_t name_len,
                                        const chc_type *t,
                                        const uint8_t *null_map,
                                        const void *inner_data,
                                        size_t n_rows, chc_err *err)
{
    size_t es = chc__bld_inner_fixed_size(t, CHC_NULLABLE);
    if (!es) return chc__err_set(err, CHC_ERR_TYPE,
        "append_nullable_fixed: type is not Nullable(<fixed>)");
    int rc = chc__bld_check_rows(bb, n_rows, err);
    if (rc != CHC_OK) return rc;
    rc = chc__bld_grow(bb, err);
    if (rc != CHC_OK) return rc;
    chc__col_entry *e = &bb->cols[bb->n_cols++];
    memset(e, 0, sizeof *e);
    e->name = name; e->name_len = name_len;
    e->type = t;
    e->kind = CHC__BLD_NULL_FIXED;
    e->n_rows = n_rows;
    e->nullable.null_map = null_map;
    e->fixed.data = inner_data;
    e->fixed.elem_size = es;
    return CHC_OK;
}

int
chc_block_builder_append_nullable_string(chc_block_builder *bb,
                                         const char *name, size_t name_len,
                                         const chc_type *t,
                                         const uint8_t *null_map,
                                         const uint64_t *inner_offsets,
                                         const uint8_t *inner_data,
                                         size_t n_rows, chc_err *err)
{
    if (!chc__bld_inner_is_string(t, CHC_NULLABLE))
        return chc__err_set(err, CHC_ERR_TYPE,
            "append_nullable_string: type is not Nullable(String)");
    int rc = chc__bld_check_rows(bb, n_rows, err);
    if (rc != CHC_OK) return rc;
    rc = chc__bld_grow(bb, err);
    if (rc != CHC_OK) return rc;
    chc__col_entry *e = &bb->cols[bb->n_cols++];
    memset(e, 0, sizeof *e);
    e->name = name; e->name_len = name_len;
    e->type = t;
    e->kind = CHC__BLD_NULL_STRING;
    e->n_rows = n_rows;
    e->nullable.null_map = null_map;
    e->str.offsets = inner_offsets;
    e->str.data = inner_data;
    e->inner_n = n_rows;
    return CHC_OK;
}

int
chc_block_builder_append_array_fixed(chc_block_builder *bb,
                                     const char *name, size_t name_len,
                                     const chc_type *t,
                                     const uint64_t *offsets,
                                     const void *values,
                                     size_t n_rows, chc_err *err)
{
    size_t es = chc__bld_inner_fixed_size(t, CHC_ARRAY);
    if (!es) return chc__err_set(err, CHC_ERR_TYPE,
        "append_array_fixed: type is not Array(<fixed>)");
    int rc = chc__bld_check_rows(bb, n_rows, err);
    if (rc != CHC_OK) return rc;
    rc = chc__bld_grow(bb, err);
    if (rc != CHC_OK) return rc;
    chc__col_entry *e = &bb->cols[bb->n_cols++];
    memset(e, 0, sizeof *e);
    e->name = name; e->name_len = name_len;
    e->type = t;
    e->kind = CHC__BLD_ARRAY_FIXED;
    e->n_rows = n_rows;
    e->array.offsets = offsets;
    e->fixed.data = values;
    e->fixed.elem_size = es;
    e->inner_n = n_rows ? (size_t) offsets[n_rows - 1] : 0;
    return CHC_OK;
}

int
chc_block_builder_append_array_string(chc_block_builder *bb,
                                      const char *name, size_t name_len,
                                      const chc_type *t,
                                      const uint64_t *offsets,
                                      const uint64_t *values_offsets,
                                      const uint8_t *values_data,
                                      size_t n_rows, chc_err *err)
{
    if (!chc__bld_inner_is_string(t, CHC_ARRAY))
        return chc__err_set(err, CHC_ERR_TYPE,
            "append_array_string: type is not Array(String)");
    int rc = chc__bld_check_rows(bb, n_rows, err);
    if (rc != CHC_OK) return rc;
    rc = chc__bld_grow(bb, err);
    if (rc != CHC_OK) return rc;
    chc__col_entry *e = &bb->cols[bb->n_cols++];
    memset(e, 0, sizeof *e);
    e->name = name; e->name_len = name_len;
    e->type = t;
    e->kind = CHC__BLD_ARRAY_STRING;
    e->n_rows = n_rows;
    e->array.offsets = offsets;
    e->str.offsets = values_offsets;
    e->str.data = values_data;
    e->inner_n = n_rows ? (size_t) offsets[n_rows - 1] : 0;
    return CHC_OK;
}

/* Walk past ndim Array(...) layers, return leaf type or NULL on
 * shape mismatch */
static const chc_type *
chc__bld_array_leaf(const chc_type *t, int ndim)
{
    while (ndim-- > 0) {
        if (!t || t->kind != CHC_ARRAY || t->n_children != 1) return NULL;
        t = t->children[0];
    }
    return t;
}

int
chc_block_builder_append_array_nested_fixed(chc_block_builder *bb,
                                            const char *name, size_t name_len,
                                            const chc_type *t,
                                            int ndim,
                                            const uint64_t * const *level_offsets,
                                            const size_t *level_offsets_len,
                                            const void *values,
                                            size_t n_rows, chc_err *err)
{
    if (ndim < 2)
        return chc__err_set(err, CHC_ERR_USAGE,
            "append_array_nested_fixed: ndim must be >= 2");
    const chc_type *leaf = chc__bld_array_leaf(t, ndim);
    if (!leaf)
        return chc__err_set(err, CHC_ERR_TYPE,
            "append_array_nested_fixed: type does not match ndim");
    size_t es = chc_type_elem_size(leaf);
    if (!es)
        return chc__err_set(err, CHC_ERR_TYPE,
            "append_array_nested_fixed: leaf is not fixed-size");
    if (n_rows != level_offsets_len[0])
        return chc__err_set(err, CHC_ERR_USAGE,
            "append_array_nested_fixed: n_rows != level_offsets_len[0]");
    int rc = chc__bld_check_rows(bb, n_rows, err);
    if (rc != CHC_OK) return rc;
    rc = chc__bld_grow(bb, err);
    if (rc != CHC_OK) return rc;
    chc__col_entry *e = &bb->cols[bb->n_cols++];
    memset(e, 0, sizeof *e);
    e->name = name; e->name_len = name_len;
    e->type = t;
    e->kind = CHC__BLD_ARRAY_NESTED_FIXED;
    e->n_rows = n_rows;
    e->nested.ndim = ndim;
    e->nested.level_offsets = level_offsets;
    e->nested.level_offsets_len = level_offsets_len;
    e->fixed.data = values;
    e->fixed.elem_size = es;
    /* inner_n holds leaf element count: last cumulative end of innermost level */
    {
        size_t      ilen = level_offsets_len[ndim - 1];
        e->inner_n = ilen ? (size_t) level_offsets[ndim - 1][ilen - 1] : 0;
    }
    return CHC_OK;
}

int
chc_block_builder_append_array_nested_string(chc_block_builder *bb,
                                             const char *name, size_t name_len,
                                             const chc_type *t,
                                             int ndim,
                                             const uint64_t * const *level_offsets,
                                             const size_t *level_offsets_len,
                                             const uint64_t *values_offsets,
                                             const uint8_t *values_data,
                                             size_t n_rows, chc_err *err)
{
    if (ndim < 2)
        return chc__err_set(err, CHC_ERR_USAGE,
            "append_array_nested_string: ndim must be >= 2");
    const chc_type *leaf = chc__bld_array_leaf(t, ndim);
    if (!leaf || leaf->kind != CHC_STRING)
        return chc__err_set(err, CHC_ERR_TYPE,
            "append_array_nested_string: leaf is not String");
    if (n_rows != level_offsets_len[0])
        return chc__err_set(err, CHC_ERR_USAGE,
            "append_array_nested_string: n_rows != level_offsets_len[0]");
    int rc = chc__bld_check_rows(bb, n_rows, err);
    if (rc != CHC_OK) return rc;
    rc = chc__bld_grow(bb, err);
    if (rc != CHC_OK) return rc;
    chc__col_entry *e = &bb->cols[bb->n_cols++];
    memset(e, 0, sizeof *e);
    e->name = name; e->name_len = name_len;
    e->type = t;
    e->kind = CHC__BLD_ARRAY_NESTED_STRING;
    e->n_rows = n_rows;
    e->nested.ndim = ndim;
    e->nested.level_offsets = level_offsets;
    e->nested.level_offsets_len = level_offsets_len;
    e->str.offsets = values_offsets;
    e->str.data = values_data;
    {
        size_t      ilen = level_offsets_len[ndim - 1];
        e->inner_n = ilen ? (size_t) level_offsets[ndim - 1][ilen - 1] : 0;
    }
    return CHC_OK;
}

int
chc_block_builder_append_json_string(chc_block_builder *bb,
                                     const char *name, size_t name_len,
                                     const chc_type *t,
                                     const uint64_t *offsets,
                                     const uint8_t *data,
                                     size_t n_rows, chc_err *err)
{
    if (!t || t->kind != CHC_JSON)
        return chc__err_set(err, CHC_ERR_TYPE,
            "append_json_string requires CHC_JSON type, got %d",
            (int) (t ? t->kind : 0));
    int rc = chc__bld_check_rows(bb, n_rows, err);
    if (rc != CHC_OK) return rc;
    rc = chc__bld_grow(bb, err);
    if (rc != CHC_OK) return rc;
    chc__col_entry *e = &bb->cols[bb->n_cols++];
    memset(e, 0, sizeof *e);
    e->name = name; e->name_len = name_len;
    e->type = t;
    e->kind = CHC__BLD_JSON_STRING;
    e->n_rows = n_rows;
    e->str.offsets = offsets;
    e->str.data = data;
    e->inner_n = n_rows;
    return CHC_OK;
}

int
chc_block_builder_append_low_cardinality_string(chc_block_builder *bb,
                                                const char *name, size_t name_len,
                                                const chc_type *t,
                                                int key_size,
                                                const void *keys,
                                                const uint64_t *dict_offsets,
                                                const uint8_t *dict_data,
                                                size_t dict_n,
                                                size_t n_rows, chc_err *err)
{
    if (!chc__bld_lc_inner_is_string(t))
        return chc__err_set(err, CHC_ERR_TYPE,
            "append_low_cardinality_string: type is not LowCardinality(String) or LowCardinality(Nullable(String))");
    if (key_size != 1 && key_size != 2 && key_size != 4 && key_size != 8)
        return chc__err_set(err, CHC_ERR_USAGE,
            "append_low_cardinality_string: key_size must be 1/2/4/8 (got %d)", key_size);
    int rc = chc__bld_check_rows(bb, n_rows, err);
    if (rc != CHC_OK) return rc;
    rc = chc__bld_grow(bb, err);
    if (rc != CHC_OK) return rc;
    chc__col_entry *e = &bb->cols[bb->n_cols++];
    memset(e, 0, sizeof *e);
    e->name = name; e->name_len = name_len;
    e->type = t;
    e->kind = CHC__BLD_LC_STRING;
    e->n_rows = n_rows;
    e->lc.key_size = key_size;
    e->lc.keys = keys;
    e->str.offsets = dict_offsets;
    e->str.data = dict_data;
    e->inner_n = dict_n;
    return CHC_OK;
}

/* -------- write helpers ---------- */

static int
chc__write_bytes(chc_io *io, const void *buf, size_t n, chc_err *err)
{
    return io->write(io->ud, buf, n, err);
}

static int
chc__write_varuint(chc_io *io, uint64_t v, chc_err *err)
{
    uint8_t b[10];
    int n = 0;
    do {
        uint8_t byte = (uint8_t) (v & 0x7f);
        v >>= 7;
        if (v) byte |= 0x80;
        b[n++] = byte;
    } while (v);
    return chc__write_bytes(io, b, (size_t) n, err);
}

static int
chc__write_u32_le(chc_io *io, uint32_t v, chc_err *err)
{
    uint8_t b[4] = { (uint8_t) v, (uint8_t) (v >> 8),
                     (uint8_t) (v >> 16), (uint8_t) (v >> 24) };
    return chc__write_bytes(io, b, 4, err);
}

static int
chc__write_u64_le(chc_io *io, uint64_t v, chc_err *err)
{
    uint8_t b[8];
    for (int i = 0; i < 8; i++) b[i] = (uint8_t) (v >> (8 * i));
    return chc__write_bytes(io, b, 8, err);
}

static int
chc__write_string(chc_io *io, const char *s, size_t n, chc_err *err)
{
    int rc = chc__write_varuint(io, (uint64_t) n, err);
    if (rc != CHC_OK) return rc;
    if (n) return chc__write_bytes(io, s, n, err);
    return CHC_OK;
}

/* Emit a contiguous u64 array as little-endian. */
static int
chc__write_u64_le_array(chc_io *io, const uint64_t *p, size_t n, chc_err *err)
{
#if CHC_BIG_ENDIAN
    for (size_t i = 0; i < n; i++) {
        int rc = chc__write_u64_le(io, p[i], err);
        if (rc != CHC_OK) return rc;
    }
    return CHC_OK;
#else
    if (!n) return CHC_OK;
    return chc__write_bytes(io, p, n * sizeof(uint64_t), err);
#endif
}

/* Emit LC keys (host BO -> LE on BE hosts). */
static int
chc__write_keys_array(chc_io *io, const void *p, size_t n, int key_size,
                      chc_err *err)
{
#if CHC_BIG_ENDIAN
    int rc;
    switch (key_size) {
    case 1: return n ? chc__write_bytes(io, p, n, err) : CHC_OK;
    case 2: { const uint16_t *a = p;
        for (size_t i = 0; i < n; i++) {
            uint8_t b[2] = { (uint8_t) a[i], (uint8_t) (a[i] >> 8) };
            if ((rc = chc__write_bytes(io, b, 2, err))) return rc;
        }
        return CHC_OK;
    }
    case 4: { const uint32_t *a = p;
        for (size_t i = 0; i < n; i++)
            if ((rc = chc__write_u32_le(io, a[i], err))) return rc;
        return CHC_OK;
    }
    case 8: { const uint64_t *a = p;
        for (size_t i = 0; i < n; i++)
            if ((rc = chc__write_u64_le(io, a[i], err))) return rc;
        return CHC_OK;
    }
    }
    return chc__err_set(err, CHC_ERR_USAGE, "bad key_size %d", key_size);
#else
    if (!n) return CHC_OK;
    return chc__write_bytes(io, p, n * (size_t) key_size, err);
#endif
}

/* Emit a String column body (varuint length + bytes, per row). */
static int
chc__write_string_body(chc_io *io, const uint64_t *offsets,
                       const uint8_t *data, size_t n, chc_err *err)
{
    uint64_t prev = 0;
    for (size_t r = 0; r < n; r++) {
        uint64_t end = offsets[r];
        uint64_t len = end - prev;
        int rc = chc__write_varuint(io, len, err);
        if (rc != CHC_OK) return rc;
        if (len) {
            rc = chc__write_bytes(io, data + prev, (size_t) len, err);
            if (rc != CHC_OK) return rc;
        }
        prev = end;
    }
    return CHC_OK;
}

/* Emit the entry's column body (no prefix). Assumes n_rows > 0. */
static int
chc__bld_write_body(chc_io *io, const chc__col_entry *e, chc_err *err)
{
    int rc;
    switch (e->kind) {
    case CHC__BLD_FIXED:
        if (e->fixed.elem_size)
            return chc__write_bytes(io, e->fixed.data,
                                    e->n_rows * e->fixed.elem_size, err);
        return CHC_OK;

    case CHC__BLD_STRING:
    case CHC__BLD_JSON_STRING:
        return chc__write_string_body(io, e->str.offsets, e->str.data,
                                      e->n_rows, err);

    case CHC__BLD_NULL_FIXED:
        if ((rc = chc__write_bytes(io, e->nullable.null_map, e->n_rows, err))) return rc;
        if (e->fixed.elem_size)
            return chc__write_bytes(io, e->fixed.data,
                                    e->n_rows * e->fixed.elem_size, err);
        return CHC_OK;

    case CHC__BLD_NULL_STRING:
        if ((rc = chc__write_bytes(io, e->nullable.null_map, e->n_rows, err))) return rc;
        return chc__write_string_body(io, e->str.offsets, e->str.data,
                                      e->n_rows, err);

    case CHC__BLD_ARRAY_FIXED:
        if ((rc = chc__write_u64_le_array(io, e->array.offsets, e->n_rows, err)))
            return rc;
        if (e->inner_n && e->fixed.elem_size)
            return chc__write_bytes(io, e->fixed.data,
                                    e->inner_n * e->fixed.elem_size, err);
        return CHC_OK;

    case CHC__BLD_ARRAY_STRING:
        if ((rc = chc__write_u64_le_array(io, e->array.offsets, e->n_rows, err)))
            return rc;
        return chc__write_string_body(io, e->str.offsets, e->str.data,
                                      e->inner_n, err);

    case CHC__BLD_ARRAY_NESTED_FIXED:
        for (int lvl = 0; lvl < e->nested.ndim; lvl++) {
            if ((rc = chc__write_u64_le_array(io, e->nested.level_offsets[lvl],
                                              e->nested.level_offsets_len[lvl], err)))
                return rc;
        }
        if (e->inner_n && e->fixed.elem_size)
            return chc__write_bytes(io, e->fixed.data,
                                    e->inner_n * e->fixed.elem_size, err);
        return CHC_OK;

    case CHC__BLD_ARRAY_NESTED_STRING:
        for (int lvl = 0; lvl < e->nested.ndim; lvl++) {
            if ((rc = chc__write_u64_le_array(io, e->nested.level_offsets[lvl],
                                              e->nested.level_offsets_len[lvl], err)))
                return rc;
        }
        return chc__write_string_body(io, e->str.offsets, e->str.data,
                                      e->inner_n, err);

    case CHC__BLD_LC_STRING: {
        uint64_t flags = 0;
        switch (e->lc.key_size) {
        case 1: flags |= 0; break;
        case 2: flags |= 1; break;
        case 4: flags |= 2; break;
        case 8: flags |= 3; break;
        }
        flags |= CHC__LC_HAS_ADDITIONAL_KEYS;
        flags |= CHC__LC_NEED_UPDATE_DICT;
        if ((rc = chc__write_u64_le(io, flags, err))) return rc;
        if ((rc = chc__write_u64_le(io, (uint64_t) e->inner_n, err))) return rc;
        if ((rc = chc__write_string_body(io, e->str.offsets, e->str.data,
                                         e->inner_n, err))) return rc;
        if ((rc = chc__write_u64_le(io, (uint64_t) e->n_rows, err))) return rc;
        return chc__write_keys_array(io, e->lc.keys, e->n_rows,
                                     e->lc.key_size, err);
    }
    }
    return chc__err_set(err, CHC_ERR_USAGE, "unknown builder kind %d", e->kind);
}

int
chc_block_write(chc_io *io, const chc_block_builder *bb,
                const chc_block_opts *opts, chc_err *err)
{
    chc_block_opts def = {0};
    if (!opts) opts = &def;

    if (opts->has_block_info) {
        int rc;
        if ((rc = chc__write_varuint(io, 1, err)) != CHC_OK) return rc;
        uint8_t ov = 0;
        if ((rc = chc__write_bytes(io, &ov, 1, err)) != CHC_OK) return rc;
        if ((rc = chc__write_varuint(io, 2, err)) != CHC_OK) return rc;
        if ((rc = chc__write_u32_le(io, (uint32_t) -1, err)) != CHC_OK) return rc;
        if ((rc = chc__write_varuint(io, 0, err)) != CHC_OK) return rc;
    }

    size_t n_rows = bb->n_rows_set ? bb->n_rows : 0;
    int rc = chc__write_varuint(io, (uint64_t) bb->n_cols, err);
    if (rc != CHC_OK) return rc;
    rc = chc__write_varuint(io, (uint64_t) n_rows, err);
    if (rc != CHC_OK) return rc;

    for (size_t i = 0; i < bb->n_cols; i++) {
        const chc__col_entry *e = &bb->cols[i];
        rc = chc__write_string(io, e->name, e->name_len, err);
        if (rc != CHC_OK) return rc;

        /* Type name: legacy STRING path has no e->type; emit "String". */
        if (e->kind == CHC__BLD_STRING && !e->type) {
            rc = chc__write_string(io, "String", 6, err);
        } else {
            char tbuf[256];
            size_t need = chc_type_format(e->type, tbuf, sizeof tbuf);
            if (need >= sizeof tbuf)
                return chc__err_set(err, CHC_ERR_USAGE,
                    "type name too long for inline buffer");
            rc = chc__write_string(io, tbuf, need, err);
        }
        if (rc != CHC_OK) return rc;

        if (opts->has_custom_serialization) {
            uint8_t z = 0;
            rc = chc__write_bytes(io, &z, 1, err);
            if (rc != CHC_OK) return rc;
        }

        if (e->n_rows == 0) continue;

        /* Per-column prefix sub-stream. LC emits a key-version u64;
         * JSON emits the SerializationObject version u64 (=1, STRING). */
        if (e->kind == CHC__BLD_LC_STRING) {
            rc = chc__write_u64_le(io, 1u, err);    /* KeysSerializationVersion */
            if (rc != CHC_OK) return rc;
        } else if (e->kind == CHC__BLD_JSON_STRING) {
            rc = chc__write_u64_le(io, 1u, err);    /* SerializationObject::STRING */
            if (rc != CHC_OK) return rc;
        }

        rc = chc__bld_write_body(io, e, err);
        if (rc != CHC_OK) return rc;
    }
    return CHC_OK;
}

#endif /* CHC_IMPLEMENTATION */

#ifdef __cplusplus
}
#endif

#endif /* CLICKHOUSE_H */
