/*
 * test_openssl_io.c -- exercise clickhouse-openssl.h against an in-process
 * TLS peer reachable over loopback.
 *
 * Generates a fresh self-signed RSA cert at startup, listens on
 * 127.0.0.1:<ephemeral>, runs the server side in a pthread, drives the
 * client side through chc_openssl_io_init's chc_io vtable.
 *
 * Compile:
 *   cc -std=c11 -O2 -I. test/test_openssl_io.c -o /tmp/chc_test_openssl_io \
 *      -lssl -lcrypto -lpthread
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#include "clickhouse-openssl.h"

static int fail_count = 0;
static const char *current_test = "";

#include "test_common.h"

/* ----- self-signed cert keypair (one per process) ----- */

static int
make_keypair(EVP_PKEY **out_pkey, X509 **out_cert)
{
    EVP_PKEY     *pkey = NULL;
    X509         *cert = NULL;
    EVP_PKEY_CTX *ctx  = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
    if (!ctx) return -1;
    if (EVP_PKEY_keygen_init(ctx) <= 0) goto fail;
    if (EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, 2048) <= 0) goto fail;
    if (EVP_PKEY_keygen(ctx, &pkey) <= 0) goto fail;
    EVP_PKEY_CTX_free(ctx); ctx = NULL;

    cert = X509_new();
    if (!cert) goto fail;
    ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
    X509_gmtime_adj(X509_getm_notBefore(cert), 0);
    X509_gmtime_adj(X509_getm_notAfter(cert), 60 * 60);
    X509_set_pubkey(cert, pkey);
    X509_NAME *name = X509_get_subject_name(cert);
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                               (const unsigned char *) "localhost", -1, -1, 0);
    X509_set_issuer_name(cert, name);
    if (!X509_sign(cert, pkey, EVP_sha256())) goto fail;

    *out_pkey = pkey;
    *out_cert = cert;
    return 0;
fail:
    EVP_PKEY_CTX_free(ctx);
    X509_free(cert);
    EVP_PKEY_free(pkey);
    return -1;
}

/* ----- server side ----- */

enum { MODE_ECHO_UPCASE, MODE_CLOSE_AFTER_HANDSHAKE, MODE_DRAIN_UNTIL_CLOSE };

typedef struct {
    int       listen_fd;
    EVP_PKEY *pkey;
    X509     *cert;
    int       mode;
} server_args;

static void *
server_thread(void *p)
{
    server_args *a = p;
    int fd = accept(a->listen_fd, NULL, NULL);
    if (fd < 0) return NULL;

    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    if (!ctx) { close(fd); return NULL; }
    SSL_CTX_use_certificate(ctx, a->cert);
    SSL_CTX_use_PrivateKey(ctx, a->pkey);
    SSL *ssl = SSL_new(ctx);
    SSL_set_fd(ssl, fd);

    if (SSL_accept(ssl) <= 0) {
        ERR_print_errors_fp(stderr);
        SSL_free(ssl);
        SSL_CTX_free(ctx);
        close(fd);
        return NULL;
    }

    if (a->mode == MODE_ECHO_UPCASE) {
        char buf[64];
        for (int i = 0; i < 2; i++) {
            int n = SSL_read(ssl, buf, 5);
            if (n != 5) break;
            for (int j = 0; j < n; j++)
                if (buf[j] >= 'a' && buf[j] <= 'z') buf[j] = (char) (buf[j] - 32);
            if (SSL_write(ssl, buf, n) != n) break;
        }
    } else if (a->mode == MODE_DRAIN_UNTIL_CLOSE) {
        char buf[256];
        while (SSL_read(ssl, buf, sizeof buf) > 0) { /* discard */ }
    }
    /* MODE_CLOSE_AFTER_HANDSHAKE falls straight to shutdown. */

    SSL_shutdown(ssl);
    SSL_free(ssl);
    SSL_CTX_free(ctx);
    close(fd);
    return NULL;
}

/* ----- socket helpers ----- */

static int
listen_loopback(int *out_port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = 0;
    if (bind(fd, (struct sockaddr *) &sa, sizeof sa) != 0) { close(fd); return -1; }
    if (listen(fd, 1) != 0) { close(fd); return -1; }
    socklen_t slen = sizeof sa;
    if (getsockname(fd, (struct sockaddr *) &sa, &slen) != 0) { close(fd); return -1; }
    *out_port = ntohs(sa.sin_port);
    return fd;
}

static int
connect_loopback(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = htons((uint16_t) port);
    if (connect(fd, (struct sockaddr *) &sa, sizeof sa) != 0) { close(fd); return -1; }
    return fd;
}

/* Pairs a fresh listening socket with a client-side SSL ready to use
 * via chc_openssl_io_init. Returns 0 on success. */
typedef struct {
    int         listen_fd;
    int         cli_fd;
    SSL_CTX    *cli_ctx;
    SSL        *cli_ssl;
    pthread_t   th;
    server_args sa;
    bool        thread_started;
} fixture;

static int
fixture_up(fixture *f, EVP_PKEY *pkey, X509 *cert, int mode)
{
    memset(f, 0, sizeof *f);
    f->cli_fd = -1;
    int port = 0;
    f->listen_fd = listen_loopback(&port);
    if (f->listen_fd < 0) return -1;

    f->sa = (server_args){
        .listen_fd = f->listen_fd, .pkey = pkey, .cert = cert, .mode = mode
    };
    if (pthread_create(&f->th, NULL, server_thread, &f->sa) != 0) return -1;
    f->thread_started = true;

    f->cli_fd = connect_loopback(port);
    if (f->cli_fd < 0) return -1;

    f->cli_ctx = SSL_CTX_new(TLS_client_method());
    if (!f->cli_ctx) return -1;
    SSL_CTX_set_verify(f->cli_ctx, SSL_VERIFY_NONE, NULL);
    f->cli_ssl = SSL_new(f->cli_ctx);
    SSL_set_fd(f->cli_ssl, f->cli_fd);
    if (SSL_connect(f->cli_ssl) <= 0) {
        ERR_print_errors_fp(stderr);
        return -1;
    }
    return 0;
}

static void
fixture_down(fixture *f)
{
    SSL_free(f->cli_ssl); f->cli_ssl = NULL;
    SSL_CTX_free(f->cli_ctx); f->cli_ctx = NULL;
    if (f->cli_fd >= 0) { close(f->cli_fd); f->cli_fd = -1; }
    if (f->thread_started) { pthread_join(f->th, NULL); f->thread_started = false; }
    if (f->listen_fd >= 0) { close(f->listen_fd); f->listen_fd = -1; }
}

/* ----- tests ----- */

static void
test_echo_roundtrip(EVP_PKEY *pkey, X509 *cert)
{
    current_test = "echo_roundtrip";
    fixture f;
    if (fixture_up(&f, pkey, cert, MODE_ECHO_UPCASE) != 0) {
        fail_count++;
        goto out;
    }

    chc_openssl_io state;
    chc_io io;
    chc_openssl_io_init(&state, &io, f.cli_ssl, NULL, NULL);
    CHECK(io.read != NULL);
    CHECK(io.write != NULL);
    CHECK(io.check_cancel == NULL);

    chc_err err = {0};
    CHECK(io.write(io.ud, "hello", 5, &err) == CHC_OK);
    char rb[8] = {0};
    size_t got = 0;
    CHECK(io.read(io.ud, rb, sizeof rb, &got, &err) == CHC_OK);
    CHECK(got == 5);
    CHECK(memcmp(rb, "HELLO", 5) == 0);

    CHECK(io.write(io.ud, "ping!", 5, &err) == CHC_OK);
    memset(rb, 0, sizeof rb);
    got = 0;
    CHECK(io.read(io.ud, rb, 5, &got, &err) == CHC_OK);
    CHECK(got == 5);
    CHECK(memcmp(rb, "PING!", 5) == 0);

out:
    fixture_down(&f);
}

static void
test_eof_after_handshake(EVP_PKEY *pkey, X509 *cert)
{
    current_test = "eof_after_handshake";
    fixture f;
    if (fixture_up(&f, pkey, cert, MODE_CLOSE_AFTER_HANDSHAKE) != 0) {
        fail_count++;
        goto out;
    }

    chc_openssl_io state;
    chc_io io;
    chc_openssl_io_init(&state, &io, f.cli_ssl, NULL, NULL);

    char rb[16];
    size_t got = 99;
    chc_err err = {0};
    int rc = io.read(io.ud, rb, sizeof rb, &got, &err);
    CHECK(rc == CHC_OK);
    CHECK(got == 0);

out:
    fixture_down(&f);
}

static bool g_cancel = false;
static bool cancel_cb(void *ud) { (void) ud; return g_cancel; }

static void
test_cancel(EVP_PKEY *pkey, X509 *cert)
{
    current_test = "cancel";
    fixture f;
    if (fixture_up(&f, pkey, cert, MODE_DRAIN_UNTIL_CLOSE) != 0) {
        fail_count++;
        goto out;
    }

    chc_openssl_io state;
    chc_io io;
    chc_openssl_io_init(&state, &io, f.cli_ssl, cancel_cb, NULL);
    CHECK(io.check_cancel != NULL);
    CHECK(io.check_cancel(io.ud) == 0);

    g_cancel = true;
    chc_err err = {0};
    char rb[8] = {0};
    size_t got = 99;
    int rc = io.read(io.ud, rb, 5, &got, &err);
    CHECK(rc == CHC_ERR_CANCELLED);

    chc_err err2 = {0};
    rc = io.write(io.ud, "x", 1, &err2);
    CHECK(rc == CHC_ERR_CANCELLED);

    g_cancel = false;

out:
    fixture_down(&f);
}

int
main(void)
{
    signal(SIGPIPE, SIG_IGN);

    EVP_PKEY *pkey = NULL;
    X509     *cert = NULL;
    if (make_keypair(&pkey, &cert) != 0) {
        fprintf(stderr, "keygen failed\n");
        return 1;
    }

    test_echo_roundtrip(pkey, cert);
    test_eof_after_handshake(pkey, cert);
    test_cancel(pkey, cert);

    X509_free(cert);
    EVP_PKEY_free(pkey);

    if (fail_count) {
        fprintf(stderr, "%d failure(s)\n", fail_count);
        return 1;
    }
    printf("ok\n");
    return 0;
}
