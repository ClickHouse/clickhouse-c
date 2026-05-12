/*
 * test_cancel.c -- coverage for the chc_io cancel hook.
 *
 * Three behaviours pinned here:
 *
 *  1. `check_cancel` set before a refill returns CHC_ERR_CANCELLED on
 *     the next read, no syscall observed.
 *
 *  2. `check_cancel` flipped between refills propagates through
 *     chc__in / chc_block_read style consumers without consuming
 *     extra bytes from the socket.
 *
 *  3. `check_cancel` flipped *while a posix read is blocked* does NOT
 *     fire until the kernel releases the read (an EINTR-driven retry
 *     in chc__posix_read does not re-poll the cancel hook). This is a
 *     documented limitation, NOT a passing assertion: callers that
 *     need wall-clock-bounded cancellation must run on a transport
 *     that delivers periodic bytes (CH server PROGRESS packets) or
 *     run on top of a non-blocking io that polls inside the read.
 *
 * Approach: open a pipe & let the test process control when bytes
 * arrive. No external clickhouse-local dependency.
 */

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#include "clickhouse-posix-io.h"

static int  fail_count = 0;
static const char *current_test = "";

#include "test_common.h"

static bool cancel_now = false;
static int  cancel_calls = 0;

static bool
cancel_cb(void *ud) {
    (void) ud;
    cancel_calls++;
    return cancel_now;
}

/*
 * Bury a buffered-reader read behind a chc_io. We don't have access to
 * the internal chc__in machinery without copying it, so test the cancel
 * hook through the public surface that uses it: chc_block_read on a
 * stream that doesn't start with a valid block header but does start
 * with a `read()` call. We only care whether the read is short-circuited
 * by the cancel hook before any bytes get consumed, & whether the
 * function returns CHC_ERR_CANCELLED in that case.
 */
static int
read_block_with_cancel(int fd, chc_block **out_block, chc_err *err) {
    chc_alloc al = chc_alloc_stdlib();
    chc_posix_io state;
    chc_io io;
    chc_posix_io_init(&state, &io, fd, cancel_cb, NULL);
    chc_block_opts opts = {0};
    return chc_block_read(&io, &al, &opts, out_block, err);
}

/* (1) Pre-cancel: cancel is set before chc_block_read starts. The first
 *     refill must see check_cancel == true & return CANCELLED without
 *     consuming bytes. */
static void
test_precancel(void) {
    current_test = "precancel";
    int p[2];
    CHECK(pipe(p) == 0);

    /* Pre-load a valid varint byte so a buggy implementation that
     * forgets to poll cancel before the read would happily consume it. */
    uint8_t one = 1;
    CHECK(write(p[1], &one, 1) == 1);

    cancel_now = true;
    cancel_calls = 0;
    chc_err err = {0};
    chc_block *b = NULL;
    int rc = read_block_with_cancel(p[0], &b, &err);

    CHECK_EQ_I64(rc, CHC_ERR_CANCELLED);
    CHECK(cancel_calls > 0);

    /* Byte still sitting in the pipe — chc didn't drain it. */
    uint8_t back = 0xff;
    ssize_t n = read(p[0], &back, 1);
    CHECK_EQ_I64(n, 1);
    CHECK_EQ_I64(back, 1);

    {
        chc_alloc al = chc_alloc_stdlib();
        if (b) chc_block_destroy(b, &al);
    }
    close(p[0]); close(p[1]);
    cancel_now = false;
}

/* (2) Cancel set between successful reads: simulate by giving the reader
 *     a closed-write pipe (so chc_block_read sees clean EOF at the block
 *     boundary, returning 0). With cancel_cb returning true ahead of the
 *     read, refill returns CANCELLED *before* attempting to detect EOF.
 *     The cancel hook is consulted for every refill. */
static void
test_cancel_observed_each_refill(void) {
    current_test = "cancel_observed_each_refill";
    int p[2];
    CHECK(pipe(p) == 0);

    cancel_now = false;
    cancel_calls = 0;

    /* First: with cancel false & write-end closed → clean EOF return. */
    close(p[1]);
    chc_err err = {0};
    chc_block *b = NULL;
    int rc = read_block_with_cancel(p[0], &b, &err);
    /* Clean EOF at block boundary surfaces as rc == 0, out == NULL. */
    CHECK_EQ_I64(rc, 0);
    CHECK(b == NULL);
    int calls_no_cancel = cancel_calls;
    CHECK(calls_no_cancel >= 1);                /* polled at least once */

    /* Second: with cancel true & write-end closed → CANCELLED, not EOF. */
    int p2[2];
    CHECK(pipe(p2) == 0);
    close(p2[1]);
    cancel_now = true;
    cancel_calls = 0;
    rc = read_block_with_cancel(p2[0], &b, &err);
    CHECK_EQ_I64(rc, CHC_ERR_CANCELLED);

    close(p[0]); close(p2[0]);
    cancel_now = false;
}

/* (3) Cancel flipped while a posix read is blocked. Demonstrates the
 *     EINTR-retry behaviour: chc__posix_read silently retries on EINTR,
 *     so a cancel flipped via a signal handler is observed only after
 *     the read returns for some other reason (data arrives, peer
 *     closes). A non-blocking transport or a refresh of check_cancel
 *     inside the EINTR retry would close this gap.
 *
 *     This test asserts the *current* behaviour so a future patch that
 *     adds an EINTR-side cancel poll surfaces as a failure here & gets
 *     promoted to a behaviour change with a deliberate version bump.
 */
static volatile sig_atomic_t alarm_fired = 0;

static void
on_sigalrm(int sig) {
    (void) sig;
    alarm_fired = 1;
    cancel_now = true;
}

static void
test_blocked_read_eintr(void) {
    current_test = "blocked_read_eintr";

    int p[2];
    CHECK(pipe(p) == 0);

    struct sigaction sa = {0};
    sa.sa_handler = on_sigalrm;
    /* No SA_RESTART: signal delivery surfaces EINTR to the read syscall,
     * giving chc__posix_read the chance to re-check cancel before
     * retrying. */
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    CHECK(sigaction(SIGALRM, &sa, NULL) == 0);

    /* Fork a child that writes one byte after 200ms so the parent's
     * blocked read eventually returns. The SIGALRM fires at 50ms; if
     * chc__posix_read polled cancel on EINTR it would return CANCELLED
     * roughly at 50ms. As-is the parent stays blocked until ~200ms
     * when the child write delivers a byte. */
    pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        close(p[0]);
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 200 * 1000 * 1000 };
        nanosleep(&ts, NULL);
        uint8_t b = 1;
        (void) write(p[1], &b, 1);
        close(p[1]);
        _exit(0);
    }
    close(p[1]);

    cancel_now = false;
    alarm_fired = 0;
    struct itimerval it = {0};
    it.it_value.tv_usec = 50 * 1000;            /* 50ms */
    CHECK(setitimer(ITIMER_REAL, &it, NULL) == 0);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    chc_err err = {0};
    chc_block *b = NULL;
    int rc = read_block_with_cancel(p[0], &b, &err);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    long elapsed_ms = (t1.tv_sec - t0.tv_sec) * 1000
                    + (t1.tv_nsec - t0.tv_nsec) / 1000000;

    CHECK(alarm_fired == 1);
    /* CURRENT (buggy) behaviour: alarm flipped cancel_now to true at
     * ~50ms but the read kept retrying through EINTR. The parent
     * therefore stays blocked until the child writes at ~200ms, & only
     * then the *next* refill sees cancel & returns CANCELLED. Pin both
     * facts so a future fix surfaces clearly:
     *   - elapsed >= 150ms (proves we waited for the child write)
     *   - final rc is CANCELLED (cancel was honoured on the eventual
     *     refill after the byte arrived)
     */
    CHECK(elapsed_ms >= 150);
    CHECK_EQ_I64(rc, CHC_ERR_CANCELLED);

    {
        chc_alloc al = chc_alloc_stdlib();
        if (b) chc_block_destroy(b, &al);
    }
    close(p[0]);
    waitpid(pid, NULL, 0);
    cancel_now = false;
}

int
main(void) {
    test_precancel();
    test_cancel_observed_each_refill();
    test_blocked_read_eintr();

    if (fail_count) {
        fprintf(stderr, "FAIL: %d check(s) failed\n", fail_count);
        return 1;
    }
    fprintf(stderr, "ok\n");
    return 0;
}
