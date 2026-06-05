# clickhouse-c

Header-only C client for the [ClickHouse](https://clickhouse.com/) Native wire
format. One core header, plus optional follow-up headers for TCP, compression,
codecs, and I/O backends.

Designed for embedding inside PostgreSQL extensions (`palloc` arena, `longjmp`)
but with no PG-specific code. Including `clickhouse.h` alone gives a pure block
decoder over caller-supplied `chc_io`, usable for reading `clickhouse-local`'s
`FORMAT Native` from a pipe with no TCP, no compression, no link-time deps beyond libc.

## Quickstart

Decode a `clickhouse local` query without TCP or compression.

```c
/* demo.c — cc -std=c11 -O2 -I. demo.c -o demo */

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#include "clickhouse-posix-io.h"

#include <stdio.h>
#include <unistd.h>

int main(void) {
    /* Spawn `clickhouse local --format Native -q "..."` and read its stdout
       via a pipe; see examples/minimal_decode.c for the full plumbing. */
    int fd = /* read end of pipe from child */;

    chc_alloc al = chc_alloc_stdlib();
    chc_posix_io state;
    chc_io io;
    chc_posix_io_init(&state, &io, fd, NULL, NULL);

    chc_block_opts opts = {};   /* clickhouse-local: no BlockInfo, no custom serialization */

    for (;;) {
        chc_block *block = NULL;
        chc_err err = {};
        if (chc_block_read(&io, &al, &opts, &block, &err) != CHC_OK) {
            fprintf(stderr, "decode: %s\n", err.msg);
            return 1;
        }
        if (!block) break;

        for (size_t r = 0; r < chc_block_n_rows(block); r++)
            for (size_t c = 0; c < chc_block_n_columns(block); c++)
                /* dispatch on chc_column_layout(chc_block_column(block, c)) */;

        chc_block_destroy(block, &al);
    }
}
```

Runnable end-to-end version: [examples/minimal_decode.c](examples/minimal_decode.c).

Required server setting (Query packet for TCP, `--` flag for `clickhouse local`):

```
output_format_native_encode_types_in_binary_format = 0
```

Without it, the server emits binary type tags & `chc_block_read` returns
`CHC_ERR_TYPE`.

## Integration

`clickhouse-c` is distributed as a flat set of headers:
each header contains both declarations & implementation, guarded by
a sentinel macro. Exactly one translation unit per consumer defines the
implementation macro & includes headers; everyone else includes for
declarations only.

```c
/* consumer/src/clickhouse.c — the one TU that links the library in */
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#include "clickhouse-posix-io.h"
/* …include clickhouse-client.h, clickhouse-compression.h here for the
   features you use. */
```

Other TUs include only the public declarations:

```c
#include "clickhouse.h"
#include "clickhouse-posix-io.h"
/* no CHC_IMPLEMENTATION here */
```

A stdlib `malloc`/`realloc`/`free` helper is available behind
`#define CHC_PROVIDE_STDLIB_ALLOC` before including `clickhouse.h`. For
`palloc`/`repalloc`/`pfree` wiring inside a real PostgreSQL extension, see
[pg_clickhouse](https://github.com/ClickHouse/pg_clickhouse) or
[pg_stat_ch](https://github.com/ClickHouse/pg_stat_ch).

## Headers

| Header | Purpose | Links |
|---|---|---|
| [`clickhouse.h`](doc/clickhouse.md) | Core: types, errors, `chc_alloc`, `chc_io`, type-name parser, block reader & writer | — |
| [`clickhouse-client.h`](doc/clickhouse-client.md) | TCP packet loop: Hello / Query / Data / EOS / Exception / Progress / Pong | — |
| [`clickhouse-async.h`](doc/clickhouse-async.md) | Ioless client: same packet loop driven by caller byte submission, no socket | — |
| [`clickhouse-compression.h`](doc/clickhouse-compression.md) | Compressed-frame layout, CityHash128, `chc_codec` dispatch, LZ4 & ZSTD adapters (opt out with `CHC_NO_LZ4` / `CHC_NO_ZSTD`) | `-llz4 -lzstd` |
| [`clickhouse-posix-io.h`](doc/clickhouse-posix-io.md) | `chc_io` over blocking `read(2)`/`write(2)` with EINTR loop & cancel hook | — |
| [`clickhouse-openssl.h`](doc/clickhouse-openssl.md) | `chc_io` over `SSL_read`/`SSL_write` | `-lssl -lcrypto` |

Each follow-up header is independent; pick what your build needs.

Per-header reference in [doc/](doc/); inline declarations & comments in
the headers themselves. Worked examples in [examples/](examples/).

## Testing

Use [`test.sh`](test.sh) to run tests:

```sh
./test.sh
```

If errors report missing headers or linker errors, use `CFLAGS` and `LDFLAGS`
to point to the appropriate directories, e.g., using [Homebrew]:

```sh
CFLAGS=-I/opt/homebrew/include LDFLAGS=-L/opt/homebrew/lib ./test.sh
```

The script runs individual tests in [test](/test/). To run individual tests,
pass their base names (without the trailing `.c`) as arguments:

```sh
./test.sh test_cancel test_block_decode
```

## Non-goals

* HTTP protocol — wrap libcurl directly.
* DNS, endpoint round-robin, connection pooling, retry/backoff.
* SSL/TLS context lifecycle. `chc_io` callbacks; caller drives OpenSSL.
* Threading. Each `chc_client` is single-threaded by design.
* Async I/O inside the library. The blocking client calls `chc_io.read`
  synchronously (the callback may do whatever it wants under the hood —
  epoll, io_uring, `WaitLatchOrSocket`). For an ioless client that performs
  no I/O at all, driven by caller byte submission in an event loop, see
  [`clickhouse-async.h`](doc/clickhouse-async.md).
* `Variant` / `Dynamic` / `JSON` / `AggregateFunction` decoding in v1. The
  upstream wire format is still shifting in 25.x / 26.x.

  [Homebrew]: https://brew.sh "The Missing Package Manager for macOS (or Linux)"
