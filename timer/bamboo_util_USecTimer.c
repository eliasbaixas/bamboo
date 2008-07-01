/*
 * Copyright (c) 2004 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

#include <jni.h>
#include <sys/time.h>

JNIEXPORT jlong JNICALL 
Java_bamboo_util_USecTimer_currentTimeMicros (JNIEnv * env, jclass clazz)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (jlong) tv.tv_sec * 1000000 + tv.tv_usec;
}

