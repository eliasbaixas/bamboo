/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include "gateway_prot.h"

#define APP_STRING "gateway_test.c $Revision: 1.9 $"
#define CLIB_STRING "rpcgen"

static void 
do_null_call (CLIENT *clnt) {
    char *null_args = NULL; 
    void *null_result; 
    printf ("Doing a null call.\n");
    null_result = bamboo_dht_proc_null_2((void*)&null_args, clnt);
    if (null_result == (void *) NULL) {
        clnt_perror (clnt, "null call failed.");
        exit (1);
    }
    printf ("Null call successful.\n");
}

static void
do_put (CLIENT *clnt, bamboo_put_args *put_args) {

    bamboo_stat     *put_result;

    printf ("Doing a put\n");

    put_args->application = APP_STRING;
    put_args->client_library = CLIB_STRING;
    put_args->ttl_sec = 3600; /* 1 hour */

    put_result = bamboo_dht_proc_put_2 (put_args, clnt);
    if (put_result == (bamboo_stat *) NULL) {
        clnt_perror (clnt, "put failed");
        exit (1);
    }

    printf ("Put successful\n");
}

static bamboo_get_res*
do_get (CLIENT *clnt, bamboo_get_args *get_args) {
    bamboo_get_res  *get_result;

    printf ("Doing a get\n");

    get_args->application = APP_STRING;
    get_args->client_library = CLIB_STRING;
    get_args->maxvals = 1;

    get_result = bamboo_dht_proc_get_2 (get_args, clnt);
    if (get_result == (bamboo_get_res *) NULL) {
        clnt_perror (clnt, "get failed");
        exit (1);
    }
    else {
        return get_result;
    }
}

static CLIENT*
connect_to_server (struct sockaddr_in *addr) {
    CLIENT *clnt;
    int sockp = RPC_ANYSOCK;
    clnt = clnttcp_create (addr, BAMBOO_DHT_GATEWAY_PROGRAM, 
            BAMBOO_DHT_GATEWAY_VERSION, &sockp, 0, 0);
    if (clnt == NULL) {
        clnt_pcreateerror ("create error");
        exit (1);
    }
    return clnt;
}

static int 
compare_values (bamboo_value *val, char* v2, int l2) {
    int i;
    if (val->bamboo_value_len != l2)
        return val->bamboo_value_len - l2;
    for (i = 0; i < val->bamboo_value_len; ++i) {
        if (val->bamboo_value_val[i] != v2[i])
            return val->bamboo_value_val[i] - v2[i];
    }
    return 0;
}

static void 
random_key (char *key_val, int key_len) {
    int i;
    key_val [0] = 0;  // make sure positive
    for (i = 1; i < key_len; ++i)
        key_val [i] = rand ();
}

static void
run_test (struct sockaddr_in *addr)
{
    CLIENT *clnt;
    char value_val1[] = "Hello, world.";
    char value_val2[] = "Goodbye, world.";
    bamboo_put_args put_args;
    bamboo_get_args get_args;
    bamboo_get_res  *get_result;
    int first;

    memset (&put_args, 0, sizeof (put_args));
    memset (&get_args, 0, sizeof (get_args));

    srand (1);

    clnt = connect_to_server (addr);
    do_null_call (clnt);

    // Do a first put.

    random_key (put_args.key, sizeof (put_args.key));
    put_args.value.bamboo_value_val = value_val1;
    put_args.value.bamboo_value_len = sizeof (value_val1);
    do_put (clnt, &put_args);

    // Check that the data's there.

    memcpy (get_args.key, put_args.key, sizeof (get_args.key));
    get_result = do_get (clnt, &get_args);

    if (get_result->values.values_len != 1) {
        printf ("Get failed: returned %d values.\n", 
                get_result->values.values_len);
        exit (1);
    }
    if (compare_values (&(get_result->values.values_val [0]),
             value_val1, sizeof (value_val1)) != 0) {
        printf ("Get failed: values don't match: %s vs %s\n", 
                value_val1,
                get_result->values.values_val [0].bamboo_value_val);
        exit (1);
    }
    printf ("Get successful.\n");

    // Do a second put with the same key.

    put_args.value.bamboo_value_val = value_val2;
    put_args.value.bamboo_value_len = sizeof (value_val2);
    do_put (clnt, &put_args);

    // Check that both values are there.

    get_result = do_get (clnt, &get_args); 
    if (get_result->values.values_len != 1) {
        printf ("Get failed: returned %d values.\n", 
                get_result->values.values_len);
        exit (1);
    }

    printf ("Get returned value %s.\n", 
            get_result->values.values_val [0].bamboo_value_val);

    if (compare_values (&(get_result->values.values_val [0]),
             value_val1, sizeof (value_val1)) == 0) {
        printf ("Get returned first value.\n");
        first = TRUE;
    }
    else if (compare_values (&(get_result->values.values_val [0]),
                    value_val2, sizeof (value_val2)) == 0) {
        printf ("Get second first value.\n");
        first = FALSE;
    }
    else {
        printf ("Get failed: returned neither value.\n");
        exit (1);
    }

    get_args.placemark.bamboo_placemark_val = 
        get_result->placemark.bamboo_placemark_val;
    get_args.placemark.bamboo_placemark_len = 
        get_result->placemark.bamboo_placemark_len;

    get_result = do_get (clnt, &get_args); 
    if (get_result->values.values_len != 1) {
        printf ("Get failed: returned %d values.\n", 
                get_result->values.values_len);
        exit (1);
    }

    printf ("Get returned value %s.\n", 
            get_result->values.values_val [0].bamboo_value_val);

    if (first) {
        if (compare_values (&(get_result->values.values_val [0]),
                value_val2, sizeof (value_val2)) != 0) {
        printf ("Get failed: second value doesn't match: %s vs %s\n", 
                value_val2,
                get_result->values.values_val [0].bamboo_value_val);
        exit (1);
        }
    }
    else if (compare_values (&(get_result->values.values_val [0]),
                value_val1, sizeof (value_val1)) != 0) {
        printf ("Get failed: second value doesn't match: %s vs %s\n", 
                value_val1,
                get_result->values.values_val [0].bamboo_value_val);
        exit (1);
    }

    printf ("Get successful.\n");

    // Do a put with a different key.

    random_key (put_args.key, sizeof (put_args.key));
    do_put (clnt, &put_args);

    // Check that the data's there.

    memcpy (get_args.key, put_args.key, sizeof (get_args.key));
    get_args.placemark.bamboo_placemark_val = NULL;
    get_args.placemark.bamboo_placemark_len = 0;
    get_result = do_get (clnt, &get_args);

    if (get_result->values.values_len != 1) {
        printf ("Get failed: returned %d values.\n", 
                get_result->values.values_len);
        exit (1);
    }
    if (compare_values (&(get_result->values.values_val [0]),
             value_val2, sizeof (value_val2)) != 0) {
        printf ("Get failed: values don't match: %s vs %s\n", 
                value_val2,
                get_result->values.values_val [0].bamboo_value_val);
        exit (1);
    }
    printf ("Get successful.\n");

    clnt_destroy (clnt);
}

static void
lookup_server (char *host, int port, struct sockaddr_in *addr) {

    struct hostent *h;
    h = gethostbyname (host); 
    if (h == NULL) {
        perror ("gethostbyname");
        exit (1);
    }

    bzero (addr, sizeof (struct sockaddr_in));
    addr->sin_family = AF_INET;
    addr->sin_port = htons (port);
    addr->sin_addr = *((struct in_addr *) h->h_addr);
}

int
main (int argc, char *argv[])
{
    struct sockaddr_in addr;

    if (argc < 2) {
        printf ("usage: %s server_host server_port\n", argv[0]);
        exit (1);
    }

    lookup_server (argv [1], atoi (argv[2]), &addr);
    run_test (&addr);
    exit (0);
}

