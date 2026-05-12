/*
 * custom_codec.c — chc_codec vtable, demonstrated without -lzstd.
 *
 * Fills the `zstd_*` slots of chc_codec by piping payloads through the
 * `zstd` CLI in a subprocess. Pointless in production (per-call fork +
 * pipe handshake dwarfs zstd itself), but useful for:
 *
 *   1. Documenting the vtable. The whole codec surface is four
 *      function pointers + a `void *ud`; pass it to chc_client via
 *      chc_client_opts.codec when compression != CHC_COMP_NONE.
 *   2. Adapting libraries clickhouse-c doesn't ship a binding for.
 *      Replace the subprocess plumbing with calls into your library;
 *      the wire-side contract is identical to chc_zstd_codec_init in
 *      clickhouse-compression.h.
 *   3. Sandboxing. The CLI runs out-of-process; a corrupt frame can't
 *      crash the parent (handy for hardened FDW deployments).
 *
 * Wire contract — quoting clickhouse.h:
 *
 *     int (*compress)(void *ud,
 *                     const void *src, size_t src_len,
 *                     void *dst, size_t *dst_cap);
 *
 *     dst_cap is IN  = capacity of dst,
 *             OUT = bytes actually written. Return 0 on success;
 *     return CHC_ERR_OOM if dst is too small (caller will grow & retry).
 *
 *     int (*decompress)(void *ud,
 *                       const void *src, size_t src_len,
 *                       void *dst, size_t dst_len);
 *
 *     dst_len is exact — caller already knows the uncompressed size
 *     from the frame header. Return 0 on success.
 *
 * Build:
 *     cc -std=c11 -O2 -I. examples/custom_codec.c -o custom_codec
 * Run:
 *     ./custom_codec   # self-test: round-trip a buffer & assert byte-equal
 */

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "clickhouse.h"

/* Fork `zstd` with `argv`, feed it `src`, read up to *cap bytes back into
 * dst. Returns 0 on success & writes bytes-produced into *cap. Returns
 * CHC_ERR_OOM if the output exceeds *cap. Returns CHC_ERR_IO on any other
 * failure (failed fork/exec, child exit != 0). */
static int
run_zstd(const char *const argv[],
         const void *src, size_t src_len,
         void *dst, size_t *cap)
{
    int in_pipe[2], out_pipe[2];
    if (pipe(in_pipe)  < 0) return CHC_ERR_IO;
    if (pipe(out_pipe) < 0) { close(in_pipe[0]); close(in_pipe[1]); return CHC_ERR_IO; }

    pid_t pid = fork();
    if (pid < 0) {
        close(in_pipe[0]); close(in_pipe[1]);
        close(out_pipe[0]); close(out_pipe[1]);
        return CHC_ERR_IO;
    }
    if (pid == 0) {
        dup2(in_pipe[0], STDIN_FILENO);
        dup2(out_pipe[1], STDOUT_FILENO);
        close(in_pipe[0]);  close(in_pipe[1]);
        close(out_pipe[0]); close(out_pipe[1]);
        execvp(argv[0], (char *const *) argv);
        _exit(127);
    }
    close(in_pipe[0]);
    close(out_pipe[1]);

    /* Stream src in. With small fixed buffers a single write would
     * deadlock against PIPE_BUF; iterate. */
    const uint8_t *p = src;
    size_t left = src_len;
    while (left) {
        ssize_t n = write(in_pipe[1], p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            close(in_pipe[1]); close(out_pipe[0]);
            waitpid(pid, NULL, 0);
            return CHC_ERR_IO;
        }
        p += n; left -= (size_t) n;
    }
    close(in_pipe[1]);

    uint8_t *q = dst;
    size_t   max = *cap;
    size_t   have = 0;
    bool     overflow = false;
    for (;;) {
        ssize_t n = read(out_pipe[0], q + have,
                         have < max ? (max - have) : 1);
        if (n < 0) {
            if (errno == EINTR) continue;
            close(out_pipe[0]);
            waitpid(pid, NULL, 0);
            return CHC_ERR_IO;
        }
        if (n == 0) break;
        if (have + (size_t) n > max) { overflow = true; break; }
        have += (size_t) n;
    }
    close(out_pipe[0]);

    int status;
    waitpid(pid, &status, 0);
    if (overflow) return CHC_ERR_OOM;
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) return CHC_ERR_IO;
    *cap = have;
    return CHC_OK;
}

static int
zstd_cli_compress(void *ud,
                  const void *src, size_t src_len,
                  void *dst, size_t *cap)
{
    int level = ud ? *(int *) ud : 3;
    char lvl[8];
    snprintf(lvl, sizeof lvl, "-%d", level);
    const char *const argv[] = { "zstd", lvl, "-q", "--no-check", NULL };
    return run_zstd(argv, src, src_len, dst, cap);
}

static int
zstd_cli_decompress(void *ud,
                    const void *src, size_t src_len,
                    void *dst, size_t dst_len)
{
    (void) ud;
    size_t cap = dst_len;
    const char *const argv[] = { "zstd", "-d", "-q", NULL };
    int rc = run_zstd(argv, src, src_len, dst, &cap);
    if (rc != CHC_OK) return rc;
    /* Wire contract: decompress is exact-size. A short read = corruption. */
    return cap == dst_len ? CHC_OK : CHC_ERR_PROTOCOL;
}

/* Build a chc_codec that uses the zstd CLI for ZSTD slots & nothing for LZ4
 * (NULL slots are fine; chc_client_init rejects compression == LZ4 if the
 * lz4_* function pointers are missing).
 *
 * `level_storage` outlives the returned codec — keep it on the same stack
 * frame as the codec, or move both into long-lived storage. */
chc_codec
chc_zstd_cli_codec(int *level_storage)
{
    chc_codec c = {0};
    c.ud              = level_storage;
    c.zstd_compress   = zstd_cli_compress;
    c.zstd_decompress = zstd_cli_decompress;
    return c;
}

/* ----- self-test ---------------------------------------------------------- */

int
main(void)
{
    static const char payload[] =
        "the quick brown fox jumps over the lazy dog. "
        "the quick brown fox jumps over the lazy dog. "
        "the quick brown fox jumps over the lazy dog. "
        "the quick brown fox jumps over the lazy dog.";
    size_t plen = sizeof payload - 1;

    int level = 3;
    chc_codec codec = chc_zstd_cli_codec(&level);

    uint8_t  compressed[256];
    size_t   ccap = sizeof compressed;
    if (codec.zstd_compress(codec.ud, payload, plen, compressed, &ccap) != CHC_OK) {
        fprintf(stderr, "compress failed\n");
        return 1;
    }
    fprintf(stderr, "compressed %zu -> %zu bytes\n", plen, ccap);

    uint8_t roundtrip[sizeof payload];
    if (codec.zstd_decompress(codec.ud, compressed, ccap, roundtrip, plen) != CHC_OK) {
        fprintf(stderr, "decompress failed\n");
        return 1;
    }
    if (memcmp(payload, roundtrip, plen) != 0) {
        fprintf(stderr, "round-trip mismatch\n");
        return 1;
    }

    fprintf(stderr, "ok\n");
    return 0;
}
