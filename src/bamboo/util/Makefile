#
# Copyright (c) 2001-2003 Regents of the University of California.
# All rights reserved.
#
# See the file LICENSE included in this distribution for details.
#

BAMBOO_HOME=../../..
include $(BAMBOO_HOME)/src/bamboo/Makefile.include

SOURCES=$(wildcard *.java)
OBJECTS=$(SOURCES:.java=.class)

all: Curry.java $(OBJECTS)

clean:
	rm -f *.class Curry.java

Curry.java: Curry.pl
	./$< > $@
