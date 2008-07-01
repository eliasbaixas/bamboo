#
# Copyright (c) 2001-2003 Regents of the University of California.
# All rights reserved.
#
# See the file LICENSE included in this distribution for details.
#

SUBDIRS = redir multicast i3 fileshare

BAMBOO_HOME = $(shell cd ../../..; pwd)
include ../Makefile.include

all:	subdirs

subdirs:
	@for dir in $(SUBDIRS); \
	do $(MAKE) -C $$dir; \
	done

clean:
	@for dir in $(SUBDIRS); \
	do $(MAKE) -C $$dir clean; \
	done

