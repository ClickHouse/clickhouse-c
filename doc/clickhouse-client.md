# clickhouse-client.h

TCP packet loop over `chc_io`. Hello / Query / Data / EOS / Exception /
Progress / Pong, including compression negotiation. Depends on
[clickhouse.h](clickhouse.md) and [clickhouse-compression.h](clickhouse-compression.md).

One `chc_client` wraps one connection. No reconnect, endpoint failover, or
DNS — caller-side concerns. Caller owns the `chc_io` (socket setup, TLS,
timeouts, cancel polling).

## Opts & handshake

```c
#define CHC_CLIENT_DEFAULT_REVISION 54459u

typedef struct chc_client_opts {
    const char *client_name;        /* default "clickhouse-c" */
    uint64_t client_version_major;
    uint64_t client_version_minor;
    uint64_t client_version_patch;
    uint64_t client_revision;       /* default CHC_CLIENT_DEFAULT_REVISION */

    const char *database;           /* default "default" */
    const char *user;               /* default "default" */
    const char *password;           /* default "" */

    chc_compression  compression;   /* CHC_COMP_NONE if codec is NULL */
    const chc_codec *codec;

    size_t read_buffer_bytes;       /* 0 = 8 KiB */
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

int  chc_client_init        (chc_client **out, const chc_client_opts *opts,
                             const chc_alloc *al, chc_io *io, chc_err *err);
void chc_client_close       (chc_client *c);
const chc_server_info *chc_client_server_info(const chc_client *c);
```

`chc_client_init` runs Hello / HelloAck synchronously. On failure the
caller may still pass the returned (NULL-on-fail) handle to
`chc_client_close`. The effective revision (min of client & server) is
exposed via `chc_client_server_info` and used to gate optional fields on
every later packet.

Compression: pass `compression = CHC_COMP_LZ4`/`CHC_COMP_ZSTD` plus a
filled `codec`. The client uncompresses incoming Data packets & compresses
outgoing ones. See [clickhouse-compression.md](clickhouse-compression.md)
for the codec vtable & the built-in LZ4 / ZSTD adapters.

## Send

```c
int chc_client_send_query (chc_client *c,
                           const char *sql,      size_t sql_len,
                           const char *query_id, size_t query_id_len,
                           chc_err *err);

int chc_client_send_data  (chc_client *c, const chc_block_builder *bb,
                           chc_err *err);

int chc_client_send_cancel(chc_client *c, chc_err *err);
int chc_client_send_ping  (chc_client *c, chc_err *err);
```

`send_query` emits the Query packet plus a trailing empty Data block — the
terminator the server uses to detect end-of-query-text. Settings list is
empty; OpenTelemetry context is empty; quota key is empty.

`send_data` with `bb == NULL` emits an empty Data block: ends an INSERT
stream, or acts as the query-text terminator (internally used by
`send_query`).

## Receive

```c
typedef enum chc_packet_kind {
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

typedef struct chc_packet {
    chc_packet_kind kind;
    chc_block      *block;        /* DATA / TOTALS / EXTREMES / LOG / PROFILE_EVENTS */
    chc_exception  *exception;    /* EXCEPTION */
    struct { uint64_t rows, bytes, total_rows,
                      written_rows, written_bytes; } progress;
    struct { uint64_t rows, blocks, bytes, rows_before_limit;
             uint8_t  applied_limit, calculated_rows_before_limit; } profile;
} chc_packet;

int  chc_client_recv_packet(chc_client *c, chc_packet *out, chc_err *err);
void chc_packet_clear      (chc_client *c, chc_packet *p);
```

Exceptions arrive as `CHC_PKT_EXCEPTION` packets — `recv_packet` returns
`CHC_OK`, not `CHC_ERR_SERVER`. Only transport-level failures return
non-OK. `TABLE_COLUMNS` body is consumed & discarded (no caller-visible
fields).

`chc_packet_clear` frees the `block` & `exception`. NULLing them on
the packet before calling transfers ownership to the caller.

```c
typedef struct chc_exception chc_exception;
struct chc_exception {
    int32_t  code;
    char    *name;          size_t name_len;
    char    *display_text;  size_t display_text_len;
    char    *stack_trace;   size_t stack_trace_len;
};

void chc_exception_free(chc_exception *e, const chc_alloc *al);
```

`written_rows` / `written_bytes` are zero pre-revision 54420.
`total_rows` is zero pre-51554. Library reads these fields when the
negotiated revision exposes them, leaving zeros otherwise.

## Threading

Each `chc_client` is single-threaded. The library calls `chc_io` callbacks
synchronously; the callbacks themselves may do whatever they want under
the hood (epoll, io_uring, `WaitLatchOrSocket`).
