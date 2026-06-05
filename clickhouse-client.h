/*
 * clickhouse-client.h -- TCP packet loop over chc_io.
 *
 * Exactly one TU must `#define CHC_IMPLEMENTATION` before including;
 * the implementation uses internal varint / io / block helpers from
 * clickhouse.h.
 *
 * Caller owns the chc_io (socket setup, TLS, timeouts, cancel polling).
 * One chc_client wraps one connection. No reconnect / endpoint failover
 * / DNS — caller-side concerns.
 */

#ifndef CLICKHOUSE_CLIENT_H
#define CLICKHOUSE_CLIENT_H

#include <stdbool.h>
#include <stdint.h>

#include "clickhouse.h"
#include "clickhouse-compression.h"  /* chc_compression enum, chc_codec struct */

#ifdef __cplusplus
extern "C" {
#endif

/* Default protocol revision the client speaks. Matches clickhouse-cpp's
 * DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS pin. */
#define CHC_CLIENT_DEFAULT_REVISION 54459u

typedef struct chc_client_opts {
    /* Identity. Defaults applied when fields are zero/NULL. */
    const char *client_name;        /* default "clickhouse-c" */
    uint64_t client_version_major;  /* default 0 */
    uint64_t client_version_minor;  /* default 0 */
    uint64_t client_version_patch;  /* default 0 */
    uint64_t client_revision;       /* default CHC_CLIENT_DEFAULT_REVISION */

    /* Hello body. */
    const char *database;           /* default "default" */
    const char *user;               /* default "default" */
    const char *password;           /* default "" */

    /* Compression. CHC_COMP_NONE if codec is NULL. */
    chc_compression compression;
    const chc_codec *codec;

    /* Internal read buffer size. 0 = use CHC_READ_BUFFER (8 KiB). */
    size_t read_buffer_bytes;
} chc_client_opts;

typedef struct chc_server_info {
    char     name[64];
    char     timezone[64];
    char     display_name[128];
    uint64_t version_major;
    uint64_t version_minor;
    uint64_t version_patch;
    uint64_t revision;              /* min(client_revision, server_revision) */
} chc_server_info;

typedef struct chc_client chc_client;

/* Performs Hello / HelloAck handshake immediately. On failure caller may
 * call chc_client_close to free any partially-allocated state. */
int  chc_client_init(chc_client **out, const chc_client_opts *opts,
                     const chc_alloc *al, chc_io *io, chc_err *err);

void chc_client_close(chc_client *c);

const chc_server_info *chc_client_server_info(const chc_client *c);

int  chc_client_send_query(chc_client *c,
                           const char *sql, size_t sql_len,
                           const char *query_id, size_t query_id_len,
                           chc_err *err);

/* Per-query setting. name / value are NUL-terminated. Matches
 * clickhouse-cpp's QuerySettingsField.flags low two bits. */
typedef struct chc_query_setting {
    const char *name;
    const char *value;
    bool        important;          /* flag bit 0 */
    bool        custom;             /* flag bit 1 (user-defined "SET custom_*=...") */
} chc_query_setting;

/* Per-query parameter (substituted into `{name:Type}` placeholders in the
 * SQL text). name / value are NUL-terminated. The wire-level flags byte is
 * always CUSTOM. The server parses value via Field::restoreFromDump, so
 * callers must format value as a typed Field literal: e.g. `'hello'` for a
 * String, `42` for an integer, `[1,2,3]` for an array. NULL is `'\\N'`.
 * (Unlike clickhouse-cpp's higher-level Client::SetParam, which auto-quotes
 * raw strings, this library passes the value through verbatim so callers
 * keep full control of typed and non-string values.) */
typedef struct chc_query_param {
    const char *name;
    const char *value;
} chc_query_param;

typedef struct chc_query_opts {
    const char *query_id;
    size_t      query_id_len;
    const chc_query_setting *settings;
    size_t                   n_settings;
    const chc_query_param   *params;
    size_t                   n_params;
} chc_query_opts;

int  chc_client_send_query_ex(chc_client *c,
                              const char *sql, size_t sql_len,
                              const chc_query_opts *opts, chc_err *err);

typedef enum chc_packet_kind {
    CHC_PKT_HELLO           = 0,
    CHC_PKT_DATA            = 1,
    CHC_PKT_EXCEPTION       = 2,
    CHC_PKT_PROGRESS        = 3,
    CHC_PKT_PONG            = 4,
    CHC_PKT_END_OF_STREAM   = 5,
    CHC_PKT_PROFILE_INFO    = 6,
    CHC_PKT_TOTALS          = 7,
    CHC_PKT_EXTREMES        = 8,
    CHC_PKT_LOG             = 10,
    CHC_PKT_TABLE_COLUMNS   = 11,
    CHC_PKT_PROFILE_EVENTS  = 14,
} chc_packet_kind;

/* CHC_PKT_EXCEPTION payload. Caller frees with chc_exception_free
 * if produced. */
typedef struct chc_exception chc_exception;
struct chc_exception {
    int32_t        code;
    char          *name;         /* allocated in chc_alloc */
    size_t         name_len;
    char          *display_text;
    size_t         display_text_len;
    char          *stack_trace;
    size_t         stack_trace_len;
};

void chc_exception_free(chc_exception *e, const chc_alloc *al);

typedef struct chc_packet {
    chc_packet_kind kind;

    /* Payload selected by kind; exactly one member is live, none for
     * PONG / END_OF_STREAM / TABLE_COLUMNS. */
    union {
        /* CHC_PKT_DATA / TOTALS / EXTREMES / LOG / PROFILE_EVENTS:
         * caller-owned chc_block, freed with chc_block_destroy. */
        chc_block *block;

        /* CHC_PKT_EXCEPTION: caller-owned, freed with chc_exception_free. */
        chc_exception *exception;

        /* CHC_PKT_PROGRESS. */
        struct {
            uint64_t rows, bytes, total_rows;
            uint64_t written_rows, written_bytes;  /* >= rev 54420 */
        } progress;

        /* CHC_PKT_PROFILE_INFO. */
        struct {
            uint64_t rows, blocks, bytes, rows_before_limit;
            uint8_t  applied_limit, calculated_rows_before_limit;
        } profile;
    };
} chc_packet;

/* Read the next packet. On exception packets the caller MUST inspect
 * out->kind == CHC_PKT_EXCEPTION; the function returns CHC_OK with the
 * exception attached, not CHC_ERR_SERVER. */
int  chc_client_recv_packet(chc_client *c, chc_packet *out, chc_err *err);

/* Free anything chc_client_recv_packet allocated for this packet:
 * the block (if any) and the exception chain (if any). Safe to call
 * with packet->{block,exception} already NULLed by the caller (after
 * the caller takes ownership). */
void chc_packet_clear(chc_client *c, chc_packet *p);

/* Send a Data block. bb == NULL emits an empty block which the server
 * interprets as "no more INSERT rows" or as the query-text terminator
 * sent at the end of every SendQuery. */
int  chc_client_send_data(chc_client *c, const chc_block_builder *bb,
                          chc_err *err);

int  chc_client_send_cancel(chc_client *c, chc_err *err);
int  chc_client_send_ping(chc_client *c, chc_err *err);

#ifdef CHC_IMPLEMENTATION

#include <stdlib.h>
#include <string.h>

/* ----- protocol revision constants (mirror clickhouse-cpp client.cpp) ----- */
#define CHC__REV_TEMPORARY_TABLES        50264u
#define CHC__REV_TOTAL_ROWS_IN_PROGRESS  51554u
#define CHC__REV_BLOCK_INFO              51903u
#define CHC__REV_CLIENT_INFO             54032u
#define CHC__REV_SERVER_TIMEZONE         54058u
#define CHC__REV_QUOTA_KEY_IN_CLIENT     54060u
#define CHC__REV_SERVER_DISPLAY_NAME     54372u
#define CHC__REV_VERSION_PATCH           54401u
#define CHC__REV_CLIENT_WRITE_INFO       54420u
#define CHC__REV_SETTINGS_AS_STRINGS     54429u
#define CHC__REV_INTERSERVER_SECRET      54441u
#define CHC__REV_OPENTELEMETRY           54442u
#define CHC__REV_DISTRIBUTED_DEPTH       54448u
#define CHC__REV_INITIAL_QUERY_START     54449u
#define CHC__REV_PARALLEL_REPLICAS       54453u
#define CHC__REV_CUSTOM_SERIALIZATION    54454u
#define CHC__REV_ADDENDUM                54458u
#define CHC__REV_QUOTA_KEY               54458u
#define CHC__REV_PARAMETERS              54459u

/* Client → server packet kinds. */
#define CHC__CLIENT_HELLO  0u
#define CHC__CLIENT_QUERY  1u
#define CHC__CLIENT_DATA   2u
#define CHC__CLIENT_CANCEL 3u
#define CHC__CLIENT_PING   4u

struct chc_client {
    const chc_alloc *al;
    chc_io          *io;
    chc_in           in;            /* persistent buffered input */

    chc_server_info  server;
    uint64_t         client_revision;
    chc_compression  compression;
    const chc_codec *codec;

    /* Meaningful only for an ioless `chc_in`.
     * io-backed refill blocks inside reader, so packet always completes,
     * leaving these at reset values across calls. */
    int              recv_in_block;  /* 0 = at packet boundary; 1 = mid block-bearing packet */
    chc_packet_kind  recv_kind;      /* committed kind for the in-progress block */
    chc_block       *recv_partial;   /* retained partial block (uncompressed resume) */
    size_t           recv_next_col;  /* resume column index */
};

/* Free retained recv-resume state. Safe at any packet boundary or teardown. */
static void
chc__client_recv_state_free(chc_client *c)
{
    if (c->recv_partial) { chc_block_destroy(c->recv_partial, c->al); c->recv_partial = NULL; }
    c->recv_in_block = 0;
    c->recv_next_col = 0;
}

void
chc_exception_free(chc_exception *e, const chc_alloc *al)
{
    if (!e) return;
    al->free(al->ud, e->name,         e->name_len         + 1);
    al->free(al->ud, e->display_text, e->display_text_len + 1);
    al->free(al->ud, e->stack_trace,  e->stack_trace_len  + 1);
    al->free(al->ud, e, sizeof *e);
}

static int
chc__read_i32_le(chc_in *in, int32_t *out, chc_err *err)
{
    uint32_t u;
    int rc = chc__read_u32_le(in, &u, err);
    if (rc != CHC_OK) return rc;
    *out = (int32_t) u;
    return CHC_OK;
}

static int
chc__client_send_hello(chc_client *c, const chc_client_opts *opts, chc_err *err)
{
    int rc;
    const char *name = opts->client_name ? opts->client_name : "clickhouse-c";
    size_t name_len = strlen(name);
    const char *db  = opts->database ? opts->database : "default";
    const char *us  = opts->user     ? opts->user     : "default";
    const char *pw  = opts->password ? opts->password : "";

    if ((rc = chc__write_varuint(c->io, CHC__CLIENT_HELLO, err))) return rc;
    if ((rc = chc__write_string (c->io, name, name_len, err)))   return rc;
    if ((rc = chc__write_varuint(c->io, opts->client_version_major, err))) return rc;
    if ((rc = chc__write_varuint(c->io, opts->client_version_minor, err))) return rc;
    if ((rc = chc__write_varuint(c->io, c->client_revision, err))) return rc;
    if ((rc = chc__write_string (c->io, db, strlen(db), err))) return rc;
    if ((rc = chc__write_string (c->io, us, strlen(us), err))) return rc;
    if ((rc = chc__write_string (c->io, pw, strlen(pw), err))) return rc;
    return CHC_OK;
}

static int
chc__copy_short(char *dst, size_t cap, const char *src, size_t len)
{
    size_t n = len < cap - 1 ? len : cap - 1;
    if (n) memcpy(dst, src, n);
    dst[n] = '\0';
    return 0;
}

/* Reads chained exception. Caller frees via chc_exception_free. */
static int
chc__read_exception(chc_client *c, chc_exception **out, chc_err *err)
{
    chc_exception *e = chc__calloc(c->al, sizeof *e, err);
    if (!e) return CHC_ERR_OOM;
    uint8_t has_nested;
    int rc;
    if ((rc = chc__read_i32_le (&c->in, &e->code, err)) ||
        (rc = chc__read_string (&c->in, &e->name,         &e->name_len,         err)) ||
        (rc = chc__read_string (&c->in, &e->display_text, &e->display_text_len, err)) ||
        (rc = chc__read_string (&c->in, &e->stack_trace,  &e->stack_trace_len,  err)) ||
        (rc = chc__read_byte   (&c->in, &has_nested, err))) {
        chc_exception_free(e, c->al);
        return rc;
    }
    *out = e;
    return CHC_OK;
}

static int
chc__client_recv_hello(chc_client *c, chc_err *err)
{
    uint64_t kind;
    int rc = chc__read_varuint(&c->in, &kind, err);
    if (rc != CHC_OK) return rc;
    if (kind == CHC_PKT_EXCEPTION) {
        chc_exception *e = NULL;
        rc = chc__read_exception(c, &e, err);
        if (rc != CHC_OK) return rc;
        chc__err_set(err, CHC_ERR_SERVER, "%s",
                     e->display_text ? e->display_text : (e->name ? e->name : ""));
        err->server_code = e->code;
        chc__copy_short(err->server_name, sizeof err->server_name,
                        e->name, e->name_len);
        chc_exception_free(e, c->al);
        return CHC_ERR_SERVER;
    }
    if (kind != CHC_PKT_HELLO)
        return chc__err_set(err, CHC_ERR_PROTOCOL,
                            "expected Hello, got %llu",
                            (unsigned long long) kind);

    char *s; size_t slen;
    if ((rc = chc__read_string(&c->in, &s, &slen, err))) return rc;
    chc__copy_short(c->server.name, sizeof c->server.name, s, slen);
    c->al->free(c->al->ud, s, slen + 1);

    if ((rc = chc__read_varuint(&c->in, &c->server.version_major, err))) return rc;
    if ((rc = chc__read_varuint(&c->in, &c->server.version_minor, err))) return rc;
    if ((rc = chc__read_varuint(&c->in, &c->server.revision,      err))) return rc;

    if (c->server.revision >= CHC__REV_SERVER_TIMEZONE) {
        if ((rc = chc__read_string(&c->in, &s, &slen, err))) return rc;
        chc__copy_short(c->server.timezone, sizeof c->server.timezone, s, slen);
        c->al->free(c->al->ud, s, slen + 1);
    }
    if (c->server.revision >= CHC__REV_SERVER_DISPLAY_NAME) {
        if ((rc = chc__read_string(&c->in, &s, &slen, err))) return rc;
        chc__copy_short(c->server.display_name, sizeof c->server.display_name, s, slen);
        c->al->free(c->al->ud, s, slen + 1);
    }
    if (c->server.revision >= CHC__REV_VERSION_PATCH) {
        if ((rc = chc__read_varuint(&c->in, &c->server.version_patch, err))) return rc;
    }
    return CHC_OK;
}

static int
chc__recv_pong(chc_client *c, chc_err *err)
{
    bool ioless = c->in.io == NULL;
    if (ioless) chc__in_checkpoint(&c->in);
    uint64_t kind;
    int rc = chc__read_varuint(&c->in, &kind, err);
    if (rc != CHC_OK) goto maybe_rewind;
    if (kind == CHC_PKT_EXCEPTION) {
        chc_exception *e = NULL;
        rc = chc__read_exception(c, &e, err);  /* frees its partial on non-OK */
        if (rc != CHC_OK) goto maybe_rewind;
        chc__err_set(err, CHC_ERR_SERVER, "%s",
                     e->display_text ? e->display_text : (e->name ? e->name : ""));
        err->server_code = e->code;
        chc__copy_short(err->server_name, sizeof err->server_name,
                        e->name, e->name_len);
        chc_exception_free(e, c->al);
        return CHC_ERR_SERVER;
    }
    if (kind != CHC_PKT_PONG)
        return chc__err_set(err, CHC_ERR_PROTOCOL, "expected Pong, got %llu",
                            (unsigned long long) kind);
    return CHC_OK;
maybe_rewind:
    if (ioless && rc == CHC_WOULD_BLOCK) chc__in_rewind(&c->in);
    return rc;
}

int
chc_client_init(chc_client **out, const chc_client_opts *opts,
                const chc_alloc *al, chc_io *io, chc_err *err)
{
    chc_client_opts def_opts = {0};
    if (!opts) opts = &def_opts;

    chc_client *c = chc__calloc(al, sizeof *c, err);
    if (!c) return CHC_ERR_OOM;
    c->al = al;
    c->io = io;
    c->client_revision = opts->client_revision ? opts->client_revision
                                               : CHC_CLIENT_DEFAULT_REVISION;
    c->compression = opts->codec ? opts->compression : CHC_COMP_NONE;
    c->codec       = opts->codec;

    int rc = chc_in_init(&c->in, io, al, opts->read_buffer_bytes, err);
    if (rc != CHC_OK) { al->free(al->ud, c, sizeof *c); return rc; }

    rc = chc__client_send_hello(c, opts, err);
    if (rc != CHC_OK) goto fail;
    rc = chc__client_recv_hello(c, err);
    if (rc != CHC_OK) goto fail;

    /* Server's effective revision is min(ours, server). After the
     * handshake we use this to gate optional fields on subsequent
     * packets. */
    if (c->server.revision > c->client_revision)
        c->server.revision = c->client_revision;

    /* Addendum: send empty quota_key. */
    if (c->server.revision >= CHC__REV_ADDENDUM) {
        rc = chc__write_string(c->io, "", 0, err);
        if (rc != CHC_OK) goto fail;
    }

    /* Probe Ping. Server-side late-stage rejections (eg invalid
     * default_database in 24.x) only surface after the Addendum is read,
     * not in the Hello reply. Without a probe, the rejection races the
     * caller's first query: caller's writes may hit ECONNRESET before the
     * exception packet is read. The Ping forces a round-trip here so the
     * exception is delivered at init time instead. Matches clickhouse-cpp's
     * SetPingBeforeQuery posture for the connection-establishment case. */
    rc = chc_client_send_ping(c, err);
    if (rc != CHC_OK) goto fail;
    rc = chc__recv_pong(c, err);
    if (rc != CHC_OK) goto fail;

    *out = c;
    return CHC_OK;

fail:
    chc_in_free(&c->in);
    al->free(al->ud, c, sizeof *c);
    *out = NULL;
    return rc;
}

void
chc_client_close(chc_client *c)
{
    if (!c) return;
    chc__client_recv_state_free(c);
    chc_in_free(&c->in);
    c->al->free(c->al->ud, c, sizeof *c);
}

const chc_server_info *
chc_client_server_info(const chc_client *c)
{
    return c ? &c->server : NULL;
}

int
chc_client_send_ping(chc_client *c, chc_err *err)
{
    return chc__write_varuint(c->io, CHC__CLIENT_PING, err);
}

int
chc_client_send_cancel(chc_client *c, chc_err *err)
{
    return chc__write_varuint(c->io, CHC__CLIENT_CANCEL, err);
}

/* Write a block body (BlockInfo + cols + rows) to a chc_io. Used for
 * both the uncompressed direct path and the compressed buffer-then-emit
 * path. */
static int
chc__client_write_block_body(chc_client *c, chc_io *sink,
                             const chc_block_builder *bb, chc_err *err)
{
    int rc;
    chc_block_opts opts = {
        .has_block_info = c->server.revision >= CHC__REV_BLOCK_INFO,
        .has_custom_serialization = c->server.revision >= CHC__REV_CUSTOM_SERIALIZATION,
    };
    if (bb) return chc_block_write(sink, bb, &opts, err);

    /* Empty block: BlockInfo + 0 cols + 0 rows. */
    if (opts.has_block_info) {
        if ((rc = chc__write_varuint(sink, 1, err))) return rc;
        uint8_t z = 0;
        if ((rc = chc__write_bytes(sink, &z, 1, err))) return rc;
        if ((rc = chc__write_varuint(sink, 2, err))) return rc;
        if ((rc = chc__write_u32_le(sink, (uint32_t) -1, err))) return rc;
        if ((rc = chc__write_varuint(sink, 0, err))) return rc;
    }
    if ((rc = chc__write_varuint(sink, 0, err))) return rc;  /* n_cols */
    if ((rc = chc__write_varuint(sink, 0, err))) return rc;  /* n_rows */
    return CHC_OK;
}

/* Write a Data packet. bb may be NULL for an empty (0 columns, 0 rows)
 * block — the terminator the server uses to detect end-of-INSERT and
 * end-of-query-text. */
static int
chc__client_write_data(chc_client *c, const chc_block_builder *bb, chc_err *err)
{
    int rc;
    if ((rc = chc__write_varuint(c->io, CHC__CLIENT_DATA, err))) return rc;
    /* Temporary table name (always empty from us). */
    if (c->server.revision >= CHC__REV_TEMPORARY_TABLES) {
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;
    }

    if (c->compression == CHC_COMP_NONE) {
        return chc__client_write_block_body(c, c->io, bb, err);
    }

    if (!c->codec)
        return chc__err_set(err, CHC_ERR_USAGE,
                            "compression enabled but codec is NULL");

    chc__mem_sink ms;
    chc_io sink_io;
    chc__mem_sink_init(&ms, &sink_io, c->al);
    rc = chc__client_write_block_body(c, &sink_io, bb, err);
    if (rc != CHC_OK) { chc__mem_sink_free(&ms); return rc; }
    rc = chc__comp_emit_chunks(c->io, c->codec, c->compression,
                               ms.buf, ms.len, c->al, err);
    chc__mem_sink_free(&ms);
    return rc;
}

int
chc_client_send_data(chc_client *c, const chc_block_builder *bb, chc_err *err)
{
    return chc__client_write_data(c, bb, err);
}

int
chc_client_send_query_ex(chc_client *c, const char *sql, size_t sql_len,
                         const chc_query_opts *opts, chc_err *err)
{
    chc_query_opts def = {0};
    if (!opts) opts = &def;

    int rc;
    if ((rc = chc__write_varuint(c->io, CHC__CLIENT_QUERY, err))) return rc;
    if ((rc = chc__write_string (c->io, opts->query_id, opts->query_id_len, err))) return rc;

    /* ClientInfo. clickhouse-cpp sends a fully-populated struct; we send
     * the minimum the server tolerates (initial fields blank, iface=TCP). */
    if (c->server.revision >= CHC__REV_CLIENT_INFO) {
        uint8_t query_kind = 1;       /* INITIAL_QUERY */
        if ((rc = chc__write_bytes (c->io, &query_kind, 1, err))) return rc;
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;  /* initial_user */
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;  /* initial_query_id */
        if ((rc = chc__write_string(c->io, "[::ffff:127.0.0.1]:0", 20, err))) return rc;
        if (c->server.revision >= CHC__REV_INITIAL_QUERY_START) {
            uint8_t z8[8] = {0};
            if ((rc = chc__write_bytes(c->io, z8, 8, err))) return rc;  /* int64 */
        }
        uint8_t iface_type = 1;       /* TCP */
        if ((rc = chc__write_bytes (c->io, &iface_type, 1, err))) return rc;
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;  /* os_user */
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;  /* client_hostname */
        if ((rc = chc__write_string(c->io, "clickhouse-c client", 19, err))) return rc;
        if ((rc = chc__write_varuint(c->io, 0, err))) return rc;     /* version_major */
        if ((rc = chc__write_varuint(c->io, 0, err))) return rc;     /* version_minor */
        if ((rc = chc__write_varuint(c->io, c->client_revision, err))) return rc;

        if (c->server.revision >= CHC__REV_QUOTA_KEY_IN_CLIENT)
            if ((rc = chc__write_string(c->io, "", 0, err))) return rc;
        if (c->server.revision >= CHC__REV_DISTRIBUTED_DEPTH)
            if ((rc = chc__write_varuint(c->io, 0, err))) return rc;
        if (c->server.revision >= CHC__REV_VERSION_PATCH)
            if ((rc = chc__write_varuint(c->io, 0, err))) return rc;  /* version_patch */
        if (c->server.revision >= CHC__REV_OPENTELEMETRY) {
            uint8_t no_otel = 0;
            if ((rc = chc__write_bytes(c->io, &no_otel, 1, err))) return rc;
        }
        if (c->server.revision >= CHC__REV_PARALLEL_REPLICAS) {
            if ((rc = chc__write_varuint(c->io, 0, err))) return rc;
            if ((rc = chc__write_varuint(c->io, 0, err))) return rc;
            if ((rc = chc__write_varuint(c->io, 0, err))) return rc;
        }
    }

    /* Per-query settings: name + varuint(flags) + value, repeated, then
     * empty-string terminator. Pre-54429 binary serialization isn't
     * implemented; the empty-list path still works because the terminator
     * is shape-compatible. */
    if (c->server.revision >= CHC__REV_SETTINGS_AS_STRINGS) {
        for (size_t i = 0; i < opts->n_settings; i++) {
            const chc_query_setting *s = &opts->settings[i];
            size_t nlen = s->name  ? strlen(s->name)  : 0;
            size_t vlen = s->value ? strlen(s->value) : 0;
            uint64_t flags = (s->important ? 1u : 0u) | (s->custom ? 2u : 0u);
            if ((rc = chc__write_string (c->io, s->name,  nlen, err))) return rc;
            if ((rc = chc__write_varuint(c->io, flags, err))) return rc;
            if ((rc = chc__write_string (c->io, s->value, vlen, err))) return rc;
        }
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;
    } else {
        if (opts->n_settings)
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "server revision %llu < %u: query settings unsupported",
                (unsigned long long) c->server.revision,
                CHC__REV_SETTINGS_AS_STRINGS);
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;
    }

    if (c->server.revision >= CHC__REV_INTERSERVER_SECRET) {
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;
    }

    /* Stages::Complete = 2. */
    if ((rc = chc__write_varuint(c->io, 2, err))) return rc;
    /* Compression state: 1 if enabled, 0 otherwise. */
    if ((rc = chc__write_varuint(c->io,
        c->compression != CHC_COMP_NONE ? 1u : 0u, err))) return rc;
    /* Query text. */
    if ((rc = chc__write_string(c->io, sql, sql_len, err))) return rc;

    /* Parameters: same shape as settings; flags always CUSTOM (bit 1). */
    if (c->server.revision >= CHC__REV_PARAMETERS) {
        for (size_t i = 0; i < opts->n_params; i++) {
            const chc_query_param *p = &opts->params[i];
            size_t nlen = p->name  ? strlen(p->name)  : 0;
            size_t vlen = p->value ? strlen(p->value) : 0;
            if ((rc = chc__write_string (c->io, p->name,  nlen, err))) return rc;
            if ((rc = chc__write_varuint(c->io, 2u, err))) return rc;
            if ((rc = chc__write_string (c->io, p->value, vlen, err))) return rc;
        }
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;
    } else if (opts->n_params) {
        return chc__err_set(err, CHC_ERR_PROTOCOL,
            "server revision %llu < %u: query parameters unsupported",
            (unsigned long long) c->server.revision, CHC__REV_PARAMETERS);
    }

    /* Finalize: send an empty Data block as the query-text terminator. */
    return chc__client_write_data(c, NULL, err);
}

int
chc_client_send_query(chc_client *c, const char *sql, size_t sql_len,
                      const char *query_id, size_t query_id_len, chc_err *err)
{
    chc_query_opts opts = {
        .query_id = query_id,
        .query_id_len = query_id_len,
    };
    return chc_client_send_query_ex(c, sql, sql_len, &opts, err);
}

void
chc_packet_clear(chc_client *c, chc_packet *p)
{
    if (!p) return;
    switch (p->kind) {
    case CHC_PKT_DATA: case CHC_PKT_TOTALS: case CHC_PKT_EXTREMES:
    case CHC_PKT_LOG:  case CHC_PKT_PROFILE_EVENTS:
        if (p->block) { chc_block_destroy(p->block, c->al); p->block = NULL; }
        break;
    case CHC_PKT_EXCEPTION:
        if (p->exception) { chc_exception_free(p->exception, c->al); p->exception = NULL; }
        break;
    default:
        break;
    }
}

/* Read the leading string a block-bearing packet carries before block body.
 * DATA/TOTALS/EXTREMES gate a temp-table name on REV_TEMPORARY_TABLES (gated=1);
 * LOG/PROFILE_EVENTS always prepend a tag (gated=0). */
static int
chc__recv_skip_lead_string(chc_client *c, int gated, chc_err *err)
{
    if (gated && c->server.revision < CHC__REV_TEMPORARY_TABLES)
        return CHC_OK;
    char *s; size_t slen;
    int rc = chc__read_string(&c->in, &s, &slen, err);
    if (rc != CHC_OK) return rc;
    c->al->free(c->al->ud, s, slen + 1);
    return CHC_OK;
}

/* Read one compressed block body from c->in into *out. Baseline: build a
 * per-call decompressor + io-backed dec_in over the (possibly ioless) raw in
 * and run the block reader. ioless: checkpoint raw at block start so a mid-
 * decompress drain rewinds and re-decompresses from frame 0 on retry. Frame-
 * granularity resumption is future work; when it lands
 * it replaces this body with a persisted decomp + ioless dec_in pump loop. */
static int
chc__recv_block_compressed(chc_client *c, const chc_block_opts *opts,
                           chc_block **out, chc_err *err)
{
    if (!c->codec)
        return chc__err_set(err, CHC_ERR_USAGE,
                            "compression enabled but codec is NULL");
    bool ioless = c->in.io == NULL;
    if (ioless) chc__in_checkpoint(&c->in);   /* raw in, at compressed block start */
    chc__decomp_src src;
    chc_io decomp_io;
    chc__decomp_src_init(&src, &c->in, c->codec, c->al, &decomp_io);
    chc_in dec_in;
    int rc = chc_in_init(&dec_in, &decomp_io, c->al, 0, err);
    if (rc != CHC_OK) { chc__decomp_src_free(&src); return rc; }
    rc = chc__block_read_in(&dec_in, c->al, opts, out, err);
    chc_in_free(&dec_in);
    chc__decomp_src_free(&src);
    if (ioless && rc == CHC_WOULD_BLOCK) chc__in_rewind(&c->in);
    return rc;
}

/* Resumable packet decoder shared by chc_client_recv_packet and
 * chc_async_recv_packet. For io-backed `in` refill blocks inside the reader.
 * Ioless-gated checkpoints/resets are skipped, and a packet always completes
 * in one call. For an ioless `in` it checkpoints at packet start and at each
 * resumable boundary so a mid-parse drain returns CHC_WOULD_BLOCK with
 * c->recv_* state retained for the next call.
 *
 * recv keeps parse state alive across CHC_WOULD_BLOCK: a Data block streamed
 * over many reads resumes at the in-progress column (uncompressed, via
 * chc__block_resume_in); compressed Data uses rewind-to-block-start. */
static int
chc__recv_packet_resumable(chc_client *c, chc_packet *out, chc_err *err)
{
    bool ioless = c->in.io == NULL;
    chc_block_opts opts = {
        .has_block_info = c->server.revision >= CHC__REV_BLOCK_INFO,
        .has_custom_serialization = c->server.revision >= CHC__REV_CUSTOM_SERIALIZATION,
    };
    int rc;
    int is_log = 0;  /* LOG/PROFILE_EVENTS: never compressed */

    memset(out, 0, sizeof *out);

    if (!c->recv_in_block) {
        if (ioless) chc__in_checkpoint(&c->in);   /* packet start */
        uint64_t kind;
        rc = chc__read_varuint(&c->in, &kind, err);
        if (rc != CHC_OK) goto maybe_rewind;

        switch (kind) {
        /* ---- control / non-block packets: parse inline ---- */
        case CHC_PKT_EXCEPTION:
            out->kind = CHC_PKT_EXCEPTION;
            rc = chc__read_exception(c, &out->exception, err);  /* frees its partial on non-OK */
            if (rc != CHC_OK) goto maybe_rewind;
            goto control_done;

        case CHC_PKT_PROGRESS:
            out->kind = CHC_PKT_PROGRESS;
            if ((rc = chc__read_varuint(&c->in, &out->progress.rows,  err)) ||
                (rc = chc__read_varuint(&c->in, &out->progress.bytes, err)))
                goto maybe_rewind;
            if (c->server.revision >= CHC__REV_TOTAL_ROWS_IN_PROGRESS)
                if ((rc = chc__read_varuint(&c->in, &out->progress.total_rows, err)))
                    goto maybe_rewind;
            if (c->server.revision >= CHC__REV_CLIENT_WRITE_INFO) {
                if ((rc = chc__read_varuint(&c->in, &out->progress.written_rows,  err)) ||
                    (rc = chc__read_varuint(&c->in, &out->progress.written_bytes, err)))
                    goto maybe_rewind;
            }
            goto control_done;

        case CHC_PKT_PONG:
            out->kind = CHC_PKT_PONG;
            goto control_done;

        case CHC_PKT_END_OF_STREAM:
            out->kind = CHC_PKT_END_OF_STREAM;
            goto control_done;

        case CHC_PKT_PROFILE_INFO:
            out->kind = CHC_PKT_PROFILE_INFO;
            if ((rc = chc__read_varuint(&c->in, &out->profile.rows,   err)) ||
                (rc = chc__read_varuint(&c->in, &out->profile.blocks, err)) ||
                (rc = chc__read_varuint(&c->in, &out->profile.bytes,  err)) ||
                (rc = chc__read_byte   (&c->in, &out->profile.applied_limit, err)) ||
                (rc = chc__read_varuint(&c->in, &out->profile.rows_before_limit, err)) ||
                (rc = chc__read_byte   (&c->in, &out->profile.calculated_rows_before_limit, err)))
                goto maybe_rewind;
            goto control_done;

        case CHC_PKT_TABLE_COLUMNS: {
            out->kind = CHC_PKT_TABLE_COLUMNS;
            /* table name + columns metadata; both varstrings, both ignored. */
            char *s; size_t slen;
            if ((rc = chc__read_string(&c->in, &s, &slen, err))) goto maybe_rewind;
            c->al->free(c->al->ud, s, slen + 1);
            if ((rc = chc__read_string(&c->in, &s, &slen, err))) goto maybe_rewind;
            c->al->free(c->al->ud, s, slen + 1);
            goto control_done;
        }

        /* ---- block-bearing: commit kind + leading string, then resume ---- */
        case CHC_PKT_DATA:    out->kind = c->recv_kind = CHC_PKT_DATA;     break;
        case CHC_PKT_TOTALS:  out->kind = c->recv_kind = CHC_PKT_TOTALS;   break;
        case CHC_PKT_EXTREMES:out->kind = c->recv_kind = CHC_PKT_EXTREMES; break;
        case CHC_PKT_LOG:     out->kind = c->recv_kind = CHC_PKT_LOG;      is_log = 1; break;
        case CHC_PKT_PROFILE_EVENTS:
            out->kind = c->recv_kind = CHC_PKT_PROFILE_EVENTS; is_log = 1; break;

        default:
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                                "unknown server packet %llu",
                                (unsigned long long) kind);
        }

        rc = chc__recv_skip_lead_string(c, !is_log, err);
        if (rc != CHC_OK) goto maybe_rewind;

        /* Kind + leading string committed; resume owns the cursor from here.
         * Do NOT rewind/reset on a mid-block would-block: resume re-parses only
         * from the in-progress column. */
        c->recv_in_block = 1;
        c->recv_partial = NULL;
        c->recv_next_col = 0;
    } else {
        out->kind = c->recv_kind;
        is_log = (c->recv_kind == CHC_PKT_LOG ||
                  c->recv_kind == CHC_PKT_PROFILE_EVENTS);
    }

    /* LOG/PROFILE_EVENTS blocks are never compressed; route through the
     * uncompressed resume path regardless of c->compression. */
    if (c->compression == CHC_COMP_NONE || is_log) {
        rc = chc__block_resume_in(&c->in, c->al, &opts,
                                  &c->recv_partial, &c->recv_next_col, err);
        if (rc == CHC_WOULD_BLOCK) return rc;  /* resume owns rewind; partial retained */
        c->recv_in_block = 0;
        if (rc != CHC_OK) {
            c->recv_partial = NULL;  /* resume already freed it */
            c->recv_next_col = 0;
            return rc;
        }
        out->block = c->recv_partial;
        c->recv_partial = NULL;
        c->recv_next_col = 0;
        if (ioless) chc_in_reset(&c->in);
        return CHC_OK;
    }

    rc = chc__recv_block_compressed(c, &opts, &out->block, err);
    if (rc == CHC_WOULD_BLOCK) return rc;
    c->recv_in_block = 0;
    if (rc != CHC_OK) return rc;
    if (ioless) chc_in_reset(&c->in);
    return CHC_OK;

control_done:
    if (ioless) chc_in_reset(&c->in);
    return CHC_OK;

maybe_rewind:
    if (ioless && rc == CHC_WOULD_BLOCK) chc__in_rewind(&c->in);
    return rc;
}

int
chc_client_recv_packet(chc_client *c, chc_packet *out, chc_err *err)
{
    return chc__recv_packet_resumable(c, out, err);
}

#endif /* CHC_IMPLEMENTATION */

#ifdef __cplusplus
}
#endif

#endif /* CLICKHOUSE_CLIENT_H */
