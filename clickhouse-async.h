/*
 * clickhouse-async.h
 *
 * Never touches a socket; caller manages bytes:
 *   - inbound socket bytes  -> chc_async_submit
 *   - outbound bytes        -> chc_async_pending_out / chc_async_consume_out
 *
 * handshake / recv_packet return CHC_WOULD_BLOCK when the input buffer
 * drains mid-parse; the caller submit more and retries. Sends only ever
 * append to *out buffer.
 *
 * No write backpressure: sends never block, never return CHC_WOULD_BLOCK, and
 * grow *out unbounded (only OOM fails). Outbound flow control is the caller's.
 * Use chc_async_pending_out: stop sending when enough pending, resume when
 * chc_async_consume_out has drained it enough.
 *
 * recv keeps parser state alive across CHC_WOULD_BLOCK (in cli.recv_*): a Data
 * block streamed over many reads resumes at the in-progress column rather than
 * re-parsing from scratch (uncompressed path, via chc__block_resume_in).
 * Compressed Data resumes at frame granularity: at most one frame is
 * re-decompressed and one column re-parsed on retry.
 */

#ifndef CLICKHOUSE_ASYNC_H
#define CLICKHOUSE_ASYNC_H

#ifdef CHC_NO_ASYNC
#  error "clickhouse-async.h needs the ioless reader; remove CHC_NO_ASYNC"
#endif

#include "clickhouse-client.h"      /* pulls clickhouse.h + clickhouse-compression.h */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct chc_async_client chc_async_client;

/* No I/O, cannot block; only OOM fails. opts->codec carries compression. */
int  chc_async_client_init(chc_async_client **out, const chc_client_opts *opts,
                           const chc_alloc *al, chc_err *err);
void chc_async_client_free(chc_async_client *c);

/* byte shuttle */
int  chc_async_submit(chc_async_client *c, const void *buf, size_t len,
                    chc_err *err);
void chc_async_pending_out(chc_async_client *c, const uint8_t **buf, size_t *len);
void chc_async_consume_out(chc_async_client *c, size_t n);

/* drive -- never block; CHC_WOULD_BLOCK = submit more / drain out */
int  chc_async_handshake(chc_async_client *c, chc_err *err);
int  chc_async_send_query(chc_async_client *c, const char *sql, size_t sql_len,
                          const char *query_id, size_t query_id_len, chc_err *err);
int  chc_async_send_data(chc_async_client *c, const chc_block_builder *bb,
                         chc_err *err);
int  chc_async_send_data_end(chc_async_client *c, chc_err *err);
int  chc_async_recv_packet(chc_async_client *c, chc_packet *out, chc_err *err);
const chc_server_info *chc_async_server_info(const chc_async_client *c);

/* Mirror chc_packet_clear: free p->block / p->exception via the client's
 * allocator. */
void chc_async_packet_clear(chc_async_client *c, chc_packet *p);

#ifdef __cplusplus
}
#endif

#endif /* CLICKHOUSE_ASYNC_H */

#ifdef CHC_IMPLEMENTATION

/* This block compiles in the same TU as clickhouse-client.h's implementation,
 * so struct chc_client and the static chc__client_* / chc__recv_packet_resumable /
 * chc__recv_pong / chc__client_recv_state_free / chc__read_* / chc__mem_sink_*
 * internals are visible. */

enum {
    CHC__HS_SEND_HELLO = 0,  /* buffer Hello, advance */
    CHC__HS_RECV_HELLO,      /* parse server Hello (resumable) */
    CHC__HS_POST_HELLO,      /* buffer addendum quota_key + Ping */
    CHC__HS_RECV_PONG,       /* parse Pong (resumable) */
    CHC__HS_DONE
};

struct chc_async_client {
    chc_client    cli;       /* driven directly; not via chc_client_init */

    chc__mem_sink out;       /* outbound write sink; cli.io points at out_io */
    chc_io        out_io;
    size_t        out_consumed;  /* bytes of out.buf the caller has drained */

    int           hs_phase;  /* CHC__HS_* */

    /* recv resumption state lives on cli (chc_client.recv_*), shared with the
     * blocking decoder; persisted across CHC_WOULD_BLOCK. */

    /* Owned copies of the Hello string fields (opts may not outlive init). */
    char *client_name;
    char *database;
    char *user;
    char *password;
    uint64_t client_version_major;
    uint64_t client_version_minor;
};

static char *
chc__async_dup(const chc_alloc *al, const char *s, chc_err *err)
{
    if (!s) return NULL;
    size_t n = strlen(s);
    char *p = al->alloc(al->ud, n + 1);
    if (!p) { chc__err_set(err, CHC_ERR_OOM, "alloc(%zu) failed", n + 1); return NULL; }
    memcpy(p, s, n + 1);
    return p;
}

static void
chc__async_free_str(const chc_alloc *al, char *s)
{
    if (s) al->free(al->ud, s, strlen(s) + 1);
}

int
chc_async_client_init(chc_async_client **out, const chc_client_opts *opts,
                      const chc_alloc *al, chc_err *err)
{
    chc_client_opts def_opts = {};
    if (!opts) opts = &def_opts;

    chc_async_client *c = chc__calloc(al, sizeof *c, err);
    if (!c) return CHC_ERR_OOM;

    c->cli.al = al;
    c->cli.client_revision = opts->client_revision ? opts->client_revision
                                                   : CHC_CLIENT_DEFAULT_REVISION;
    c->cli.compression = opts->codec ? opts->compression : CHC_COMP_NONE;
    c->cli.codec       = opts->codec;
    /* Seed server.revision so block/packet framing is well-defined before the
     * handshake completes; recv after handshake uses min(client, server). */
    c->cli.server.revision = c->cli.client_revision;

    chc__mem_sink_init(&c->out, &c->out_io, al);
    c->cli.io = &c->out_io;

    int rc = chc_in_init_ioless(&c->cli.in, al);
    if (rc != CHC_OK) { al->free(al->ud, c, sizeof *c); return rc; }

    /* Stash Hello inputs; opts strings are borrowed and need not outlive init. */
    c->client_version_major = opts->client_version_major;
    c->client_version_minor = opts->client_version_minor;
    if ((opts->client_name && !(c->client_name = chc__async_dup(al, opts->client_name, err))) ||
        (opts->database    && !(c->database    = chc__async_dup(al, opts->database,    err))) ||
        (opts->user        && !(c->user        = chc__async_dup(al, opts->user,        err))) ||
        (opts->password    && !(c->password    = chc__async_dup(al, opts->password,    err)))) {
        chc_async_client_free(c);
        return CHC_ERR_OOM;
    }

    c->hs_phase = CHC__HS_SEND_HELLO;
    *out = c;
    return CHC_OK;
}

void
chc_async_client_free(chc_async_client *c)
{
    if (!c) return;
    const chc_alloc *al = c->cli.al;
    chc__client_recv_state_free(&c->cli);
    chc_in_free(&c->cli.in);
    chc__mem_sink_free(&c->out);
    chc__async_free_str(al, c->client_name);
    chc__async_free_str(al, c->database);
    chc__async_free_str(al, c->user);
    chc__async_free_str(al, c->password);
    al->free(al->ud, c, sizeof *c);
}

int
chc_async_submit(chc_async_client *c, const void *buf, size_t len, chc_err *err)
{
    return chc_in_submit(&c->cli.in, buf, len, err);
}

void
chc_async_pending_out(chc_async_client *c, const uint8_t **buf, size_t *len)
{
    *buf = c->out.buf + c->out_consumed;
    *len = c->out.len - c->out_consumed;
}

void
chc_async_consume_out(chc_async_client *c, size_t n)
{
    size_t avail = c->out.len - c->out_consumed;
    if (n > avail) n = avail;
    c->out_consumed += n;
    /* Fully drained: reset length so the sink doesn't grow unbounded. Keep the
     * allocation (no realloc-shrink). */
    if (c->out_consumed == c->out.len) {
        c->out.len = 0;
        c->out_consumed = 0;
    }
}

static void
chc__async_hello_opts(const chc_async_client *c, chc_client_opts *o)
{
    memset(o, 0, sizeof *o);
    o->client_name = c->client_name;
    o->client_version_major = c->client_version_major;
    o->client_version_minor = c->client_version_minor;
    o->client_revision = c->cli.client_revision;
    o->database = c->database;
    o->user = c->user;
    o->password = c->password;
}

int
chc_async_handshake(chc_async_client *c, chc_err *err)
{
    int rc;
    chc_client *cli = &c->cli;

    for (;;) {
        switch (c->hs_phase) {
        case CHC__HS_SEND_HELLO: {
            chc_client_opts o;
            chc__async_hello_opts(c, &o);
            rc = chc__client_send_hello(cli, &o, err);
            if (rc != CHC_OK) return rc;
            c->hs_phase = CHC__HS_RECV_HELLO;
            continue;
        }

        case CHC__HS_RECV_HELLO:
            chc__in_checkpoint(&cli->in);
            rc = chc__client_recv_hello(cli, err);
            if (rc == CHC_WOULD_BLOCK) { chc__in_rewind(&cli->in); return rc; }
            if (rc != CHC_OK) return rc;  /* server exception / protocol */
            chc_in_reset(&cli->in);
            if (cli->server.revision > cli->client_revision)
                cli->server.revision = cli->client_revision;
            c->hs_phase = CHC__HS_POST_HELLO;
            continue;

        case CHC__HS_POST_HELLO:
            if (cli->server.revision >= CHC__REV_ADDENDUM) {
                rc = chc__write_string(cli->io, "", 0, err);  /* quota_key */
                if (rc != CHC_OK) return rc;
            }
            rc = chc_client_send_ping(cli, err);
            if (rc != CHC_OK) return rc;
            c->hs_phase = CHC__HS_RECV_PONG;
            continue;

        case CHC__HS_RECV_PONG:
            rc = chc__recv_pong(cli, err);  /* owns checkpoint/rewind */
            if (rc != CHC_OK) return rc;
            chc_in_reset(&cli->in);
            c->hs_phase = CHC__HS_DONE;
            continue;

        case CHC__HS_DONE:
        default:
            return CHC_OK;
        }
    }
}

int
chc_async_send_query(chc_async_client *c, const char *sql, size_t sql_len,
                     const char *query_id, size_t query_id_len, chc_err *err)
{
    return chc_client_send_query(&c->cli, sql, sql_len, query_id, query_id_len, err);
}

int
chc_async_send_data(chc_async_client *c, const chc_block_builder *bb, chc_err *err)
{
    return chc_client_send_data(&c->cli, bb, err);
}

int
chc_async_send_data_end(chc_async_client *c, chc_err *err)
{
    return chc_client_send_data(&c->cli, NULL, err);
}

const chc_server_info *
chc_async_server_info(const chc_async_client *c)
{
    return &c->cli.server;
}

void
chc_async_packet_clear(chc_async_client *c, chc_packet *p)
{
    chc_packet_clear(&c->cli, p);
}

/* The recv packet decoder is shared with the blocking client
 * (chc__recv_packet_resumable, clickhouse-client.h): the ioless `cli.in`
 * makes it checkpoint at packet/column boundaries and yield CHC_WOULD_BLOCK
 * on underrun, with c->cli.recv_* carrying parse state across calls. */
int
chc_async_recv_packet(chc_async_client *c, chc_packet *out, chc_err *err)
{
    return chc__recv_packet_resumable(&c->cli, out, err);
}

#endif /* CHC_IMPLEMENTATION */
