/*
 * Copyright (c) 2001-2003 Regents of the University of California.
 * All rights reserved.
 *
 * See the file LICENSE included in this distribution for details.
 */

#include "bamboo_util_GetTimeOfDayCC.h"
#include "gettimeofday_cc.h"

JNIEXPORT jlong 
JNICALL Java_bamboo_util_GetTimeOfDayCC_currentTimeMillis (JNIEnv * e, 
                                                           jclass c) {
  struct timeval tv;
  gettimeofday_cc(&tv, NULL);
  return (jlong)(((tv.tv_sec * (jlong)1000000) + tv.tv_usec)/1000);
}

