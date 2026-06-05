#!/usr/bin/env bash
set -uo pipefail

cd "$(dirname "$0")" || exit 1

# liburing present? probe once; gates the real test_async_uring body.
have_liburing() {
    printf '#include <liburing.h>\nint main(void){return 0;}\n' \
        | cc -x c ${CFLAGS:-} ${LDFLAGS:-} -o /dev/null - -luring >/dev/null 2>&1
}
if have_liburing; then uring=1; else uring=0; fi

# Per-test output buffered to its own log; main thread replays logs in test
# order so parallel runs stay readable. .fail marker => that test failed.
logdir=$(mktemp -d "${TMPDIR:-/tmp}/chc_test.XXXXXX")
trap 'rm -rf "$logdir"' EXIT

run_one() {
    local name=$1
    local src=test/test_${name}.c
    local bin=/tmp/chc_test_${name}
    local log=$logdir/$name.log

    if [[ ! -f $src ]]; then
        echo "no such test: $name" > "$log"
        : > "$logdir/$name.fail"
        return 2
    fi

    local libs=() defs=()
    [[ $name == client_tcp ]] && libs+=(-llz4 -lzstd)
    [[ $name == ioless ]] && libs+=(-llz4)
    [[ $name == async_compressed ]] && libs+=(-llz4)
    [[ $name == openssl_io ]] && libs+=(-lssl -lcrypto -lpthread)
    # async_uring: enable real body when liburing available, else skip stub
    if [[ $name == async_uring && $uring == 1 ]]; then
        defs+=(-DCHC_ASYNC_URING_TEST)
        libs+=(-luring -llz4)
    fi

    {
        echo "== $name =="
        if cc -std=c11 -D_POSIX_C_SOURCE=200809L -D_DARWIN_C_SOURCE \
              -O2 -Wall -Wextra -Wno-unused-parameter -I. \
              "${defs[@]}" ${CFLAGS:-} ${LDFLAGS:-} \
              "$src" -o "$bin" "${libs[@]}"; then
            "$bin" || { echo "(exit $?)"; : > "$logdir/$name.fail"; }
        else
            : > "$logdir/$name.fail"
        fi
    } > "$log" 2>&1
}
export -f run_one
export uring logdir CFLAGS LDFLAGS

tests=()
if (( $# == 0 )); then
    for src in test/test_*.c; do
        n=${src#test/test_}
        tests+=("${n%.c}")
    done
else
    for t in "$@"; do
        tests+=("${t#test_}")
    done
fi

# Independent binaries, distinct ports/temp dirs => safe to build + run in
# parallel. Override width with JOBS=.
jobs=${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}
printf '%s\n' "${tests[@]}" \
    | xargs -P "$jobs" -I{} bash -c 'run_one "$1"' _ {}

fails=0
for t in "${tests[@]}"; do
    [[ -f $logdir/$t.log ]] && cat "$logdir/$t.log"
    [[ -f $logdir/$t.fail ]] && fails=$((fails + 1))
done

if (( fails > 0 )); then
    echo "$fails test(s) failed" >&2
    exit 1
fi
