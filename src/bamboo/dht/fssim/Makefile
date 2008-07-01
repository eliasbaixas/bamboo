#
# Copyright (c) 2001-2003 Regents of the University of California.
# All rights reserved.
#
# See the file LICENSE included in this distribution for details.
#

BAMBOO_HOME=../../../..
include $(BAMBOO_HOME)/src/bamboo/Makefile.include

SOURCES=$(wildcard *.java)
OBJECTS=$(SOURCES:.java=.class)
OBJECTS:=$(RPC_SRC:.java=.class) $(OBJECTS)

GNUPLOTS=$(wildcard *.gnuplot)
GRAPHS=$(GNUPLOTS:.gnuplot=.eps)

all: $(OBJECTS) 

graphs: $(GRAPHS)

%.eps: %.gnuplot %.dat
	gnuplot $<

test-1-control.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test1 control > $@

test-1-rate.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test1 rate > $@

test-1-virtual.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test1 virtual > $@

test-1-storage.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test1 storage > $@

test-1-commitment.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test1 commitment > $@

test-2-control.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test2 control > $@

test-2-rate.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test2 rate > $@

test-2-virtual.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test2 virtual > $@

test-2-storage.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test2 storage > $@

test-2-commitment.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test2 commitment > $@

test-3-rate.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test3 rate > $@

test-3-virtual.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test3 virtual > $@

test-3-storage.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test3 storage > $@

test-4-storage.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test4 storage > $@

test-4-virtual.dat: $(OBJECTS)
	../../../../bin/run-java -mx256M bamboo.dht.fssim.Test4 virtual > $@

test-4-rate.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test4 rate > $@

test-5-virtual.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test5 virtual > $@

test-7-virtual.dat: $(OBJECTS)
	../../../../bin/run-java bamboo.dht.fssim.Test7 virtual > $@

perf.eps: accept.sum reject.sum shift.sum
	gnuplot perf.gnuplot

accept.sum: $(DATS)
	grep accept *.dat | awk '{ print $$2,$$3; }' | sort -n > accept.sum

reject.sum: $(DATS)
	grep reject *.dat | awk '{ print $$2,$$3; }' | sort -n > reject.sum

shift.sum: $(DATS)
	grep shift *.dat | awk '{ print $$2,$$3; }' | sort -n > shift.sum


clean:
	rm -f *.class *.eps *.dat
