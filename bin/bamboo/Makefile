#
# Copyright (c) 2001-2003 Regents of the University of California.
# All rights reserved.
#
# See the file LICENSE included in this distribution for details.
#

SUBDIRS = sim www vis dmgr db dht lss api router util \
          softscribe vivaldi transport openhash

BAMBOO_HOME = $(shell cd ../..; pwd)
include Makefile.include

ifdef JAVAHOME
    JAR=$(JAVAHOME)/bin/jar 
else
    JAR=$(shell which jar)
endif

all:	precompile subdirs

precompile:
	$(MAKE) -C util Curry.java
	$(MAKE) -C dht bamboo_value.java

subdirs:
	@for dir in $(SUBDIRS); \
	do $(MAKE) -C $$dir; \
	done

clean:
	@for dir in $(SUBDIRS); \
	do $(MAKE) -C $$dir clean; \
	done

jar:
	(cd $(BAMBOO_HOME)/src; $(JAR) cvf $(BAMBOO_HOME)/jar/bamboo.jar `find . -name "*.class"` > /dev/null;)

