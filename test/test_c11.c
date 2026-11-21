/*
 * test_c11.c -- every header, implementation included, builds as ISO C11:
 * pedantic diagnostics become errors from here on.
 */

#pragma GCC diagnostic error "-Wpedantic"

#include <stdio.h>

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"
#include "clickhouse-async.h"
#include "clickhouse-posix-io.h"
#include "clickhouse-openssl.h"

int
main(void)
{
    printf("ok\n");
    return 0;
}
