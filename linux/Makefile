#
# Makefile for tools 
# Author: Aki Nakao 2002
# Copyright: Princeton University 2004
#

BAMBOO_HOME=../..
include $(BAMBOO_HOME)/src/bamboo/Makefile.include

LIBGCC=/usr/lib/gcc-lib/i386-linux/2.95.4/libgcc.a
CFLAGS=-Wall -g
INCLUDES=-I$(JAVAHOME)/include 
INCLUDES+=-I$(JAVAHOME)/include/linux
INCLUDES+=-I/usr/include/glib-2.0
INCLUDES+=-I/usr/lib/glib-2.0/include
LIBGLIB=/usr/lib/libglib-2.0.a
DEFS=

COMPILE=$(CC) $(DEFS) $(INCLUDES) $(CFLAGS)

all: libGetTimeOfDayCC.so libheapprof.so

clean:
	rm *.o libGetTimeOfDayCC.so bamboo_util_GetTimeOfDayCC.h

bamboo_util_GetTimeOfDayCC.h: $(BAMBOO_HOME)/src/bamboo/util/GetTimeOfDayCC.java
	CLASSPATH=$(CLASSPATH) $(JAVAHOME)/bin/javah \
	bamboo.util.GetTimeOfDayCC
    
libGetTimeOfDayCC.so: $(LIBGCC) gettimeofday_cc.o bamboo_util_GetTimeOfDayCC.o 
	ld -shared -o $@ gettimeofday_cc.o bamboo_util_GetTimeOfDayCC.o $(LIBGCC) 

libheapprof.so: $(LIBGCC) $(LIBGLIB) heapprof.o
	ld -shared -o $@ heapprof.o $(LIBGCC) $(LIBGLIB)

heapprof.o: ../heapprof/heapprof.c
	$(COMPILE) -c $< 

%.o:%.c
	$(COMPILE) -c $< 

bamboo_util_GetTimeOfDayCC.c: bamboo_util_GetTimeOfDayCC.h
