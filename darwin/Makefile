
BAMBOO_HOME=../..
include $(BAMBOO_HOME)/src/bamboo/Makefile.include

INCLUDES=  -I/System/Library/Frameworks/JavaVM.framework/Headers
INCLUDES+= -I/sw/include/glib-2.0
INCLUDES+= -I/sw/lib/glib-2.0/include
LIBS=/sw/lib/libglib-2.0.dylib

all: libheapprof.jnilib libUSecTimer.jnilib

libheapprof.jnilib: heapprof.o
	gcc -dynamiclib -flat_namespace -o $@ $< -L/sw/lib -lglib-2.0 

heapprof.o: ../heapprof/heapprof.c
	gcc -c $(INCLUDES) $<

bamboo_util_USecTimer.h: $(BAMBOO_HOME)/src/bamboo/util/USecTimer.java
	CLASSPATH=$(CLASSPATH) $(JAVAHOME)/bin/javah bamboo.util.USecTimer
 
bamboo_util_USecTimer.o: ../timer/bamboo_util_USecTimer.c
	gcc -c -I/System/Library/Frameworks/JavaVM.framework/Headers $<

libUSecTimer.jnilib: bamboo_util_USecTimer.o
	gcc -dynamiclib -flat_namespace -o $@ $<

bamboo_util_USecTimer.c: bamboo_util_USecTimer.h

