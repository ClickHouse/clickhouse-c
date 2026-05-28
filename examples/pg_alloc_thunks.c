/*
 * pg_alloc_thunks.c — palloc/repalloc/pfree wired into chc_alloc.
 *
 * Drop-in for a PostgreSQL extension. Copy this file into the extension's
 * src/, expose `pg_chc_alloc` to the rest of the extension, & pass &pg_chc_alloc
 * to every clickhouse-c entry point.
 *
 * Memory model:
 *   * Allocations ride on CurrentMemoryContext at the time of the call.
 *   * For long-lived state (chc_client, chc_block) the caller is expected to
 *     switch into the cursor's MemoryContext before any chc_*_init / read
 *     call, then switch back. See `cursor_alloc_example` below.
 *   * palloc ereport(ERROR)s on OOM, which siglongjmps. clickhouse-c never
 *     holds transient state across function return, so longjmp is safe:
 *     - persistent state (chc_block, chc_client) sits in the cursor's
 *       MemoryContext, freed by MemoryContextReset on transaction abort;
 *     - in-flight reads have already had their bytes attached to that same
 *       MemoryContext, so they're freed too.
 *   * pg_pfree is a real free, not a no-op: palloc-managed chunks support it.
 *     Library calls free on every explicit per-call buffer it owns, so giving
 *     palloc a real free shrinks peak memcxt size on long-running cursors.
 *
 * Reset-callback hygiene:
 *   * chc_block_destroy & chc_client_close are documented safe to call from
 *     a MemoryContextCallback. They invoke chc_alloc.free only; the thunks
 *     below never ereport, never syscall. Register a callback per cursor:
 *
 *         MemoryContextCallback *cb = MemoryContextAlloc(cur_cxt, sizeof *cb);
 *         cb->func = cursor_reset_cb;
 *         cb->arg  = cur;
 *         MemoryContextRegisterResetCallback(cur_cxt, cb);
 *
 *     and in cursor_reset_cb:
 *
 *         if (cur->block)  chc_block_destroy(cur->block, &pg_chc_alloc);
 *         if (cur->client) chc_client_close(cur->client);
 *
 *     Order matters: destroy the block first; chc_client_close also tears
 *     down its read buffer, which a still-live block doesn't depend on but
 *     reads cleanest top-down.
 *
 * What we deliberately don't do:
 *   * Wrap palloc with TRY/CATCH. PG's longjmp path is the contract; the
 *     allocator failing means the transaction is aborting, & the surrounding
 *     PG_TRY in the FDW callback handles it (just like any other palloc).
 *   * Track "old_bytes" for accounting. PG's MemoryContext stats already
 *     cover it.
 */

#include "postgres.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

#include "clickhouse.h"

static void *
pg_chc_palloc(void *ud, size_t n)
{
    (void) ud;
    return palloc(n);
}

static void *
pg_chc_repalloc(void *ud, void *p, size_t old_bytes, size_t new_bytes)
{
    (void) ud;
    (void) old_bytes;
    /* PG's repalloc rejects NULL; route through palloc on first grow. */
    return p ? repalloc(p, new_bytes) : palloc(new_bytes);
}

static void
pg_chc_pfree(void *ud, void *p, size_t bytes)
{
    (void) ud;
    (void) bytes;
    /* pfree(NULL) is undefined in PG; guard. */
    if (p) pfree(p);
}

const chc_alloc pg_chc_alloc = {
    .ud      = NULL,
    .alloc   = pg_chc_palloc,
    .realloc = pg_chc_repalloc,
    .free    = pg_chc_pfree,
};

/*
 * Usage sketch — call sites in the FDW.
 *
 * MemoryContext  cur_cxt;
 * chc_client    *client;
 *
 * cur_cxt = AllocSetContextCreate(CurrentMemoryContext,
 *                                 "pg_clickhouse cursor",
 *                                 ALLOCSET_DEFAULT_SIZES);
 *
 * MemoryContext  old = MemoryContextSwitchTo(cur_cxt);
 * chc_err        err = {0};
 * chc_client_opts opts = {
 *     .client_name = "pg_clickhouse", .user = "default", .database = "default",
 *     .compression = CHC_COMP_LZ4, .codec = &lz4_codec,
 * };
 * if (chc_client_init(&client, &opts, &pg_chc_alloc, &io, &err) != CHC_OK) {
 *     MemoryContextSwitchTo(old);
 *     ereport(ERROR,
 *             (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
 *              errmsg("clickhouse-c: %s", err.msg)));
 * }
 * MemoryContextSwitchTo(old);
 *
 * Register a reset callback so a downstream ereport(ERROR) cleans up:
 *
 * MemoryContextCallback *cb = MemoryContextAlloc(cur_cxt, sizeof *cb);
 * cb->func = cursor_reset_cb;   // calls chc_block_destroy + chc_client_close
 * cb->arg  = cur;
 * MemoryContextRegisterResetCallback(cur_cxt, cb);
 */
