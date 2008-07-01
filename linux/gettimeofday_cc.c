//
// File: gettimeofday_cc.c
//
// Author: Akihiro Nakao, Princeton University, 2004
//
// Copyright (c) 2004, Princeton University Board of Regents
//
// Note: gettimeofday replacement using cycle counter
//

// All rights reserved.

// Redistribution and use in source and binary forms, 
// with or without modification, are permitted provided that 
// the following conditions are met:
//
// Redistributions of source code must retain the above 
// copyright notice, this list of conditions and the 
// following disclaimer. 
// Redistributions in binary form must reproduce the above 
// copyright notice, this list of conditions and the following
// disclaimer in the documentation and/or other materials 
// provided with the distribution. 
// Neither the name of the  Princeton University nor the names
// of its contributors may be used to endorse or promote 
// products derived from this software without specific prior 
// written permission. 
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND 
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, 
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF 
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE 
// DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS 
// BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT 
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE 
// OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS 
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//

#ifndef __USE_BSD
#define __USE_BSD
#endif // __USE_BSD

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif // _GNU_SOURCE

#include <stdio.h>
#include <sys/types.h>
#include <sys/unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <math.h>

// macros
#define rdtsc(low, high)\
asm volatile("rdtsc":"=a" (low), "=d" (high))
#define get_cc(cc) \
do\
{\
   unsigned long __cc_low__,__cc_high__;\
   cc=0;\
   rdtsc(__cc_low__,__cc_high__);\
   cc = __cc_high__;\
   cc = (cc << 32) + __cc_low__;\
}\
while(0)


static void gettimeofday_cc_init(struct timezone* tz);
static double get_cpu_speed(char* filename);

static unsigned long long gettimeofday_cc_offset=0;
static double cpu_speed=1;
static int initialized=0;
static struct timeval tv_init;

// Note: we have 64bits for cycle counter
// it will wrap around after 2 ^ 64 / 4 GHz = 136 years on 4GHz CPU

static void
gettimeofday_cc_init(struct timezone* tz)
{
   get_cc(gettimeofday_cc_offset);
   cpu_speed=get_cpu_speed("/proc/cpuinfo");
   initialized = 1;
   gettimeofday(&tv_init, tz);
}

int
gettimeofday_cc(struct timeval *rtv, struct timezone *tz)
{
   unsigned long long cc_now;
   unsigned long long diff;
   unsigned long long usec;
   struct timeval tv;

   if (!initialized)
   {
      gettimeofday_cc_init(tz);
   }

   if (!rtv)
   {
      return -1;
   }

   // ignore timezone
   get_cc(cc_now);

   if (cc_now < gettimeofday_cc_offset)
   {
      // cc has been wrapped around
      diff = cc_now + (~(unsigned long long)0-gettimeofday_cc_offset);

      // we may want to do this
      // gettimeofday_cc_init(tz) 
   }
   else
   {
      diff = cc_now - gettimeofday_cc_offset;
   }

   usec = (unsigned long long)(diff / cpu_speed);
   tv.tv_sec = usec / 1000000;
   tv.tv_usec = usec % 1000000;

   timeradd(&tv_init,&tv,rtv);
   return 0;
}

double
get_cpu_speed(char* filename)
{
   char *line=0;
   size_t len =0;
   ssize_t num;
   char* ptr=0;
   double speed=0;

    // read /proc/cpuinfo and get cpu speed

   FILE* fp=fopen(filename, "r");

   if (!fp)
   {
      fprintf(stderr, "gettimeofday_cc: could not get cpu speed\n");
      return 1;
   }

  while ((num = getline(&line, &len, fp)) != -1) 
   {
      // FIXME: more strict parsing is necessary
      ptr=strtok(line,":\n");
      if (ptr && strstr(ptr, "cpu MHz"))
      {
         ptr=strtok(0," \r\t\n");
         speed = strtod(ptr,0);
      }
   }
   if (line)
      free(line);
   fclose(fp);

   return speed;
}

