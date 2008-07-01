/*
 * Copyright (c) 2004 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 *
 * $Id: heapprof.c,v 1.8 2004/07/18 17:05:30 bkarp Exp $
 *
 * A Hotspot pluggin to find memory leaks.  Don't even think about reading
 * this file without first reading
 *
 *   http://java.sun.com/j2se/1.4.2/docs/guide/jvmpi/jvmpi.html
 *
 * Author: Sean Rhea
 */

#include <fcntl.h>
#include <unistd.h>
#include <jni.h>
#include <jvmpi.h>
#include <regex.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/time.h>
#include <assert.h>
#include <glib.h>      /* for g_hash_table_* functions */

#define DEBUG 0

/****************************************************************************
 *                                 Globals                                  *
 ****************************************************************************/

static JVMPI_RawMonitor lock;
static JavaVM *jvm;
static JVMPI_Interface *jvmpi;

/****************************************************************************
 *                                  Time                                    *
 ****************************************************************************/

static unsigned long long now_us() {
    struct timeval tv;
    int res = gettimeofday(&tv, NULL);
    assert(! res);
    return ((unsigned long long) tv.tv_sec) * 1000 * 1000 + tv.tv_usec;
}

/****************************************************************************
 *                                 Classes                                  *
 ****************************************************************************/

typedef struct {
    char *name;
} cinfo_t;

static cinfo_t *cinfo_new(const char *name) 
{
    cinfo_t *result = (cinfo_t*) calloc(1, sizeof(cinfo_t));
    assert(result);
    result->name = strdup(name);
    return result;
}

static void cinfo_delete(cinfo_t *cinfo) {
    assert(cinfo);
    free(cinfo->name);
    free(cinfo);
}

static cinfo_t* CINFO_CLASS;
static cinfo_t* CINFO_BOOLEAN;
static cinfo_t* CINFO_BYTE;
static cinfo_t* CINFO_CHAR;
static cinfo_t* CINFO_SHORT;
static cinfo_t* CINFO_INT;
static cinfo_t* CINFO_LONG;
static cinfo_t* CINFO_FLOAT;
static cinfo_t* CINFO_DOUBLE;
static GHashTable *classes;

static void init_classes()
{
    CINFO_CLASS = cinfo_new("JVMPI_CLASS");
    CINFO_BOOLEAN = cinfo_new("JVMPI_BOOLEAN");
    CINFO_BYTE = cinfo_new("JVMPI_BYTE");
    CINFO_CHAR = cinfo_new("JVMPI_CHAR");
    CINFO_SHORT = cinfo_new("JVMPI_SHORT");
    CINFO_INT = cinfo_new("JVMPI_INT");
    CINFO_LONG = cinfo_new("JVMPI_LONG");
    CINFO_FLOAT = cinfo_new("JVMPI_FLOAT");
    CINFO_DOUBLE = cinfo_new("JVMPI_DOUBLE");
}

static cinfo_t* find_cinfo(jint is_array, jobjectID cid) 
{
    cinfo_t *result = NULL;
    switch (is_array) {
        case JVMPI_NORMAL_OBJECT:	
            result = g_hash_table_lookup(classes, cid);
            if (! result) {
                jint res;
                if (DEBUG) {
                    fprintf(stderr, "requesting class %p; is_array=0x%x\n", 
                            cid, is_array);
                }
                res = jvmpi->RequestEvent(JVMPI_EVENT_CLASS_LOAD, cid);
                assert(res == JVMPI_SUCCESS);
                result = g_hash_table_lookup(classes, cid);
                assert(result);
            }
            break;
        case JVMPI_CLASS:   result = CINFO_CLASS;   break;
        case JVMPI_BOOLEAN: result = CINFO_BOOLEAN; break;
        case JVMPI_BYTE:    result = CINFO_BYTE;    break;
        case JVMPI_CHAR:    result = CINFO_CHAR;    break;
        case JVMPI_SHORT:   result = CINFO_SHORT;   break;
        case JVMPI_INT:     result = CINFO_INT;     break;
        case JVMPI_LONG:    result = CINFO_LONG;    break;
        case JVMPI_FLOAT:   result = CINFO_FLOAT;   break;
        case JVMPI_DOUBLE:  result = CINFO_DOUBLE;  break;
        default:
            assert(0);
    } 
    return result;
}

/****************************************************************************
 *                                 Methods                                  *
 ****************************************************************************/

typedef struct {
    cinfo_t *cinfo;
    char *name;
    char *signature;
    char *printname;
    int lineno;
} method_t;

static method_t *method_new(cinfo_t* cinfo, char *name, char *signature, 
                            int lineno) 
{
    int clen, mlen;
    method_t *result = (method_t*) calloc(1, sizeof(method_t));
    assert(result);
    result->cinfo = cinfo;
    result->name = strdup(name);
    assert(result->name);
    result->signature = strdup(signature);
    assert(result->signature);
    result->lineno = lineno;
    clen = MIN(strlen(cinfo->name),45);
    mlen = MIN(strlen(name),15);
    result->printname = (char*) calloc(clen+mlen+2, sizeof(char));
    assert(result->printname);
    strncpy(result->printname, result->cinfo->name, clen);
    result->printname[clen] = '.';
    strncpy(result->printname+clen+1, result->name, mlen);
    result->printname[clen+mlen+1] = '\0';
    return result;
}

static void method_delete(method_t *method) 
{
    free(method->name);
    free(method->signature);
    free(method->printname);
    free(method);
}

static GHashTable *methods;

typedef struct _mstack_t mstack_t;
struct _mstack_t {
   jmethodID method;
   mstack_t *next;
};

static mstack_t *empty_stack = NULL;

static mstack_t *stack_push(mstack_t *stack, jmethodID id) {
    mstack_t *result = calloc (1, sizeof(mstack_t));
    result->next = stack;
    result->method = id;
    return result;
}

static mstack_t *stack_pop(mstack_t *stack) {
    mstack_t *result = stack->next;
    free(stack);
    return result;
}

/****************************************************************************
 *                            Allocation Sites                              *
 ****************************************************************************/

typedef struct {
    jmethodID method_id;
    cinfo_t *cinfo;
} asite_key_t;

typedef struct {
    asite_key_t key;
    int object_cnt;
    int total_object_bytes;
} asite_t;

static asite_t *asite_new(jmethodID method_id, cinfo_t *cinfo) 
{
    asite_t *result = (asite_t*) calloc(1, sizeof(asite_t));
    assert(result);
    assert(cinfo);
    result->key.cinfo = cinfo;
    result->key.method_id = method_id;
    result->object_cnt = 0;
    result->total_object_bytes = 0;
    return result;
}

static void asite_delete(asite_t *asite) 
{
    free(asite);
}

static GHashTable *asites;

static guint asite_key_hash (gconstpointer key) 
{
    const asite_key_t *k = (const asite_key_t*) key;
    assert(k);
    return (int) k->method_id ^ (int) k->cinfo;
}

static gboolean asite_key_equals (gconstpointer a, gconstpointer b) 
{
    const asite_key_t *k1 = (const asite_key_t*) a;
    const asite_key_t *k2 = (const asite_key_t*) b;
    assert(k1);
    assert(k2);
    return (k1->method_id == k2->method_id) && (k1->cinfo == k2->cinfo);
}

static asite_t *lookup_asite(mstack_t *stack, cinfo_t *cinfo)
{
    jmethodID method_id = (stack != empty_stack) ? stack->method : 0;
    asite_t *result;
    asite_key_t key;
    key.method_id = method_id;
    key.cinfo = cinfo;
    result = (asite_t*) g_hash_table_lookup(asites, &key);
    if (!result) {
        result = asite_new(method_id, cinfo);
        g_hash_table_insert(asites, (asite_key_t*) result, result);
    }
    return result;
}

/****************************************************************************
 *                                 Objects                                  *
 ****************************************************************************/

typedef struct {
    int size;
    asite_t *asite;
    cinfo_t *cinfo;
    GHashTable *arena;
} oinfo_t;

static oinfo_t *oinfo_new(int size, cinfo_t *cinfo, asite_t *asite, 
                          GHashTable *arena) 
{
    oinfo_t *result = (oinfo_t*) calloc(1, sizeof(oinfo_t));
    assert(result);
    result->size = size;
    result->cinfo = cinfo;
    result->asite = asite;
    result->arena = arena;
    return result;
}

static void oinfo_delete(oinfo_t *oinfo) 
{
    free(oinfo);
}

static GHashTable *objects;
static GHashTable *arenas;  // maps int->GHashTable of arena's oids

/****************************************************************************
 *                             Event Handlers                               *
 ****************************************************************************/

static void obj_alloc_event(JNIEnv *env, jobjectID cid, int is_array, int size,
                            jobjectID oid, jint aid, int requested)
{
    asite_t *asite;
    cinfo_t *cinfo;
    oinfo_t *oinfo;
    GHashTable *arena;
    mstack_t *stack;

    if (! requested) 
        jvmpi->RawMonitorEnter(lock);

    cinfo = find_cinfo(is_array, cid);
    if (! cinfo) {
        fprintf(stderr, 
                "unknown class %p in obj_alloc_event; is_array=0x%x.\n", 
                cid, is_array);
        fflush(stderr);
        exit(1);
    }

    stack = (mstack_t*) jvmpi->GetThreadLocalStorage (env);
    asite = lookup_asite (stack, cinfo);
    assert(asite);
    if (DEBUG) {
        fprintf(stderr, "Allocated %s from:\n", cinfo->name);
        while (stack != empty_stack) {
            jmethodID method_id = stack->method;
            method_t *method = (method_t*) 
                g_hash_table_lookup(methods, (gpointer) method_id);
            if (method) {
                fprintf(stderr, "  %s.%s:%d\n", method->cinfo->name, 
                        method->name, method->lineno);
            }
            stack = stack->next;
        }
    }
    asite->object_cnt++;
    asite->total_object_bytes += size;
    arena = g_hash_table_lookup(arenas, (gpointer) aid);
    if (!arena) {
        arena = g_hash_table_new(g_direct_hash, g_direct_equal);
        g_hash_table_insert(arenas, (gpointer) aid, arena);
    }
    oinfo = oinfo_new(size, cinfo, asite, arena);
    assert(! g_hash_table_lookup(objects, oid));
    g_hash_table_insert(objects, oid, oinfo);
    assert(! g_hash_table_lookup(arena, oid));
    g_hash_table_insert(arena, oid, oid);

    if (! requested) 
        jvmpi->RawMonitorExit(lock);
}

static void obj_free_event(JNIEnv *env, jobjectID oid)
{
    oinfo_t *oinfo;

    oinfo = g_hash_table_lookup(objects, oid);
    if (oinfo) {
        // TODO: otherwise, can't we look it up with a call to RequestEvent?
        gboolean res;
        res = g_hash_table_remove(objects, oid);
        assert(res);
        assert(oinfo->asite);
        oinfo->asite->object_cnt--;
        oinfo->asite->total_object_bytes -= oinfo->size;
        res = g_hash_table_remove(oinfo->arena, oid);
        assert(res);
        oinfo_delete(oinfo);
    }
}

static void obj_move_event(
        jobjectID oid, jint aid, jobjectID new_oid, jint new_aid)
{
    gboolean res;
    cinfo_t *cinfo;
    GHashTable *arena;
    oinfo_t *oinfo;

    cinfo = g_hash_table_lookup(classes, oid);
    if (cinfo) {
        gboolean res = g_hash_table_remove(classes, oid);
        assert(res);
        assert(g_hash_table_lookup(classes, new_oid) == NULL);
        g_hash_table_insert(classes, new_oid, cinfo);
    }
    oinfo = g_hash_table_lookup(objects, oid);
    if (oinfo) {
        res = g_hash_table_remove(objects, oid);
        assert(res);
        res = g_hash_table_remove(oinfo->arena, oid);
        assert(res);
        g_hash_table_insert(objects, new_oid, oinfo);
        assert(res);
        arena = g_hash_table_lookup(arenas, (gpointer) new_aid);
        if (!arena) {
            arena = g_hash_table_new(g_direct_hash, g_direct_equal);
            g_hash_table_insert(arenas, (gpointer) new_aid, arena);
        }
        assert(! g_hash_table_lookup(arena, new_oid));
        g_hash_table_insert(arena, new_oid, new_oid);
        oinfo->arena = arena;
    }
    else {
        // If we weren't aware of it before, there's no need to look up the
        // old oid, but we should look up the new oid so that we can account
        // for it.
        jint res = jvmpi->RequestEvent(JVMPI_EVENT_OBJ_ALLOC, new_oid);
        assert(res == JVMPI_SUCCESS);
    }
}

static void remove_object_cb(gpointer oid, gpointer foo, gpointer bar) 
{
    gboolean res;
    oinfo_t *oinfo;

    oinfo = g_hash_table_lookup(objects, oid);
    assert(oinfo);
    res = g_hash_table_remove(objects, oid);
    assert(res);
    assert(oinfo->asite);
    oinfo->asite->object_cnt--;
    oinfo->asite->total_object_bytes -= oinfo->size;
    oinfo_delete(oinfo);
}

static void delete_arena_event(int aid) 
{
    gboolean res;
    GHashTable *arena;

    arena = g_hash_table_lookup(arenas, (gpointer) aid);
    if (arena) {
        g_hash_table_foreach(arena, remove_object_cb, NULL);
        g_hash_table_destroy(arena);
        res = g_hash_table_remove(arenas, (gpointer) aid);
        assert(res);
    }
}

static unsigned long long last_gc;
static regex_t *class_filter;
static FILE *log_file;
static int log_fd;

static void print_counts_cb(gpointer key, gpointer asite_tmp, gpointer time)
{
    asite_t *asite = (asite_t*) asite_tmp;
    assert (asite);
    if (asite->object_cnt > 0) {
        method_t *method;
        char cbuf[46];
        char *classname;
        char *allocname;
        assert(asite->key.cinfo);
        assert(asite->key.cinfo->name);
        if (strlen(asite->key.cinfo->name) > 45) {
            strncpy(cbuf, asite->key.cinfo->name, 45);
            cbuf[45] = '\0';
            classname = cbuf;
        }
        else {
            classname = asite->key.cinfo->name;
        }
        method = (method_t*) g_hash_table_lookup(methods, 
                (gpointer) asite->key.method_id);
        if (method) 
            allocname = method->printname;
        else 
            allocname = "???.???";

        fprintf(log_file, "%10u %-45s %-61s %8d %12d\n", 
                (unsigned) time, classname, allocname,
                asite->object_cnt, asite->total_object_bytes);
    }
}

static void print_counts (void) {
    unsigned long long tmp = now_us();
    unsigned time = (unsigned) (tmp / 1000 / 1000);
    fseek(log_file, 0, SEEK_SET);
    if (log_fd)
        ftruncate(log_fd, 0);
    last_gc = tmp;
    g_hash_table_foreach(asites, print_counts_cb, (gpointer) time);
    fflush(log_file);
}

static void gc_finish_event(void) 
{
    print_counts();
    jvmpi->RawMonitorExit(lock);
}

static void gc_start_event(void) 
{
    jvmpi->RawMonitorEnter(lock);
}

static void class_load_event(const char *name, jobjectID cid, int num_methods, 
                             JVMPI_Method *meths, int requested) {
    int i;
    cinfo_t *cinfo;
    if (DEBUG) {
        fprintf(stderr, "class load %s, %p, %d\n", 
                name, cid, (requested ? 1 : 0));
    }
    if (! requested)
        jvmpi->RawMonitorEnter(lock);
    cinfo = g_hash_table_lookup(classes, (gpointer) cid);
    if (! cinfo) {
        cinfo = cinfo_new(name);
        g_hash_table_insert(classes, (gpointer) cid, cinfo);
    }
    for (i = 0; i < num_methods; ++i) {
        /* fprintf(stderr, "method %d %s %s\n", 
                meths [i].method_id,
                meths [i].method_name,
                meths [i].method_signature); */
        method_t *method;
        method = (method_t*) g_hash_table_lookup(methods, 
                (gpointer) meths [i].method_id);
        if (! method) {
            method = method_new(cinfo, meths [i].method_name,
                                meths [i].method_signature, 
                                meths [i].start_lineno);
            g_hash_table_insert(methods, (gpointer) meths [i].method_id,
                                method);
        }
    }
    if (! requested)
        jvmpi->RawMonitorExit(lock);
}

static void class_unload_event(jobjectID cid) {
    gboolean res;
    cinfo_t *cinfo;
    jvmpi->RawMonitorEnter(lock);
    cinfo = (cinfo_t*) g_hash_table_lookup(classes, (gpointer) cid);
    assert(cinfo);
    cinfo_delete(cinfo);
    res = g_hash_table_remove(classes, (gpointer) cid);
    assert(res);
    jvmpi->RawMonitorExit(lock);
}

static void method_entry_event(JNIEnv *env, jmethodID method_id) {
    mstack_t *stack = (mstack_t*) jvmpi->GetThreadLocalStorage (env);
    stack = stack_push(stack, method_id);
    jvmpi->SetThreadLocalStorage (env, stack);
}

static void method_exit_event(JNIEnv *env, jmethodID method_id) {
    mstack_t *stack = (mstack_t*) jvmpi->GetThreadLocalStorage (env);
    if (stack)
        jvmpi->SetThreadLocalStorage (env, stack_pop(stack));
}

static void notify_event(JVMPI_Event *event)
{   
    switch(event->event_type) {
        case JVMPI_EVENT_CLASS_LOAD:
        case JVMPI_EVENT_CLASS_LOAD | JVMPI_REQUESTED_EVENT:
            class_load_event(
                    event->u.class_load.class_name,
                    event->u.class_load.class_id, 
                    event->u.class_load.num_methods, 
                    event->u.class_load.methods, 
                    event->event_type & JVMPI_REQUESTED_EVENT);
            break;
        case JVMPI_EVENT_CLASS_UNLOAD:
            class_unload_event(event->u.class_unload.class_id);
            break;
        case JVMPI_EVENT_OBJ_ALLOC:
        case JVMPI_EVENT_OBJ_ALLOC | JVMPI_REQUESTED_EVENT:
            obj_alloc_event(
                    event->env_id,
                    event->u.obj_alloc.class_id,
                    event->u.obj_alloc.is_array,
                    event->u.obj_alloc.size,
                    event->u.obj_alloc.obj_id,
                    event->u.obj_alloc.arena_id,
                    event->event_type & JVMPI_REQUESTED_EVENT);
            break;
        case JVMPI_EVENT_OBJ_FREE:
            obj_free_event(event->env_id, event->u.obj_free.obj_id);
            break;
        case JVMPI_EVENT_OBJ_MOVE:
            obj_move_event(
                    event->u.obj_move.obj_id,
                    event->u.obj_move.arena_id,
                    event->u.obj_move.new_obj_id,
                    event->u.obj_move.new_arena_id);
            break;
        case JVMPI_EVENT_DELETE_ARENA:
            delete_arena_event(event->u.delete_arena.arena_id);
            break;
        case JVMPI_EVENT_GC_START:
            gc_start_event();
            break;
        case JVMPI_EVENT_GC_FINISH:
            gc_finish_event();
            break;
        case JVMPI_EVENT_METHOD_ENTRY:
            method_entry_event(event->env_id, event->u.method.method_id);
            break;
        case JVMPI_EVENT_METHOD_EXIT:
            method_exit_event(event->env_id, event->u.method.method_id);
            break;
        default:
            assert(0);
    }
}

/****************************************************************************
 *                             Initialization                               *
 ****************************************************************************/

JNIEXPORT jint JNICALL JVM_OnLoad(JavaVM *vm, char *options, void *reserved)
{
    int res;
    char *log_file_path = "/tmp/heapprof.log";
    jvm = vm;

    if (options) {
        fprintf(stderr, "options=\"%s\"\n", options);

        if ((strlen(options) > 4) && (strncmp(options, "log=", 4) == 0)) {
            log_file_path = options + 4;
        }
    }

    last_gc = now_us();

    class_filter = (regex_t*) calloc(1, sizeof(regex_t));
    res = regcomp(class_filter, 
                  "^(bamboo|ostore|org\\.apache|com\\.sleepycat)\\..*", 
                  REG_EXTENDED | REG_NOSUB);
    assert(!res);

    if (log_file_path) {
         log_fd = open(log_file_path, O_WRONLY | O_CREAT, 0664);
         if (!log_fd) {
             fprintf(stderr, "Could not open log file: %s\n", log_file_path);
             fflush(stderr);
             exit(1);
         }
         log_file = fdopen(log_fd, "w");
    }
    else {
        log_file = stderr;
        log_fd = 0;
    }

    classes = g_hash_table_new(g_direct_hash, g_direct_equal);
    init_classes();

    asites = g_hash_table_new(asite_key_hash, asite_key_equals);
    methods = g_hash_table_new(g_direct_hash, g_direct_equal);
    objects = g_hash_table_new(g_direct_hash, g_direct_equal);
    arenas = g_hash_table_new(g_direct_hash, g_direct_equal);

    res = (*jvm)->GetEnv(jvm, (void**)&jvmpi, JVMPI_VERSION_1);
    if (res < 0) 
        return JNI_ERR;
    jvmpi->NotifyEvent = notify_event;
    lock = jvmpi->RawMonitorCreate("_lock");

    jvmpi->RawMonitorEnter(lock);
    if (jvmpi->EnableEvent(JVMPI_EVENT_GC_START, NULL) != JVMPI_SUCCESS) 
        return JNI_ERR;
    if (jvmpi->EnableEvent(JVMPI_EVENT_GC_FINISH, NULL) != JVMPI_SUCCESS) 
        return JNI_ERR;
    if (jvmpi->EnableEvent(JVMPI_EVENT_OBJ_ALLOC, NULL) != JVMPI_SUCCESS) 
        return JNI_ERR;
    if (jvmpi->EnableEvent(JVMPI_EVENT_OBJ_MOVE, NULL) != JVMPI_SUCCESS) 
        return JNI_ERR;
    if (jvmpi->EnableEvent(JVMPI_EVENT_OBJ_FREE, NULL) != JVMPI_SUCCESS) 
        return JNI_ERR;
    if (jvmpi->EnableEvent(JVMPI_EVENT_DELETE_ARENA, NULL) != JVMPI_SUCCESS) 
        return JNI_ERR;
    if (jvmpi->EnableEvent(JVMPI_EVENT_CLASS_LOAD, NULL) != JVMPI_SUCCESS) 
        return JNI_ERR;
    if (jvmpi->EnableEvent(JVMPI_EVENT_CLASS_UNLOAD, NULL) != JVMPI_SUCCESS) 
        return JNI_ERR;
    if (jvmpi->EnableEvent(JVMPI_EVENT_METHOD_ENTRY, NULL) != JVMPI_SUCCESS) 
        return JNI_ERR;
    if (jvmpi->EnableEvent(JVMPI_EVENT_METHOD_EXIT, NULL) != JVMPI_SUCCESS) 
        return JNI_ERR;
    jvmpi->RawMonitorExit(lock);

    return JNI_OK;
}

