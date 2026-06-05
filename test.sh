#!/usr/bin/env bash
set -uo pipefail

cd "$(dirname "$0")" || exit 1

# liburing present? probe once; gates the real test_async_uring body.
have_liburing() {
    printf '#include <liburing.h>\nint main(void){return 0;}\n' \
        | cc -x c ${CFLAGS:-} ${LDFLAGS:-} -o /dev/null - -luring >/dev/null 2>&1
}
if have_liburing; then uring=1; else uring=0; fi

run_one() {
    local name=$1
    local src=test/test_${name}.c
    local bin=/tmp/chc_test_${name}

    if [[ ! -f $src ]]; then
        echo "no such test: $name" >&2
        return 2
    fi

    local libs=() defs=()
    [[ $name == client_tcp ]] && libs+=(-llz4 -lzstd)
    [[ $name == ioless ]] && libs+=(-llz4)
    [[ $name == openssl_io ]] && libs+=(-lssl -lcrypto -lpthread)
    # async_uring: enable real body when liburing available, else skip stub
    if [[ $name == async_uring && $uring == 1 ]]; then
        defs+=(-DCHC_ASYNC_URING_TEST)
        libs+=(-luring -llz4)
    fi

    cc -std=c11 -D_POSIX_C_SOURCE=200809L -D_DARWIN_C_SOURCE \
       -O2 -Wall -Wextra -Wno-unused-parameter -I. \
        "${defs[@]}" ${CFLAGS:-} ${LDFLAGS:-} \
       "$src" -o "$bin" "${libs[@]}" || return 1

    echo "== $name =="
    "$bin"
}

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

fails=0
for t in "${tests[@]}"; do
    run_one "$t" || fails=$((fails + 1))
done

if (( fails > 0 )); then
    echo "$fails test(s) failed" >&2
    exit 1
fi
