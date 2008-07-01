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

RPC_SRC = \
	gateway_protClient.java \
	gateway_prot.java \
	bamboo_value.java \
	bamboo_stat.java \
	bamboo_put_args.java \
	bamboo_placemark.java \
	bamboo_key.java \
	bamboo_get_res.java \
	bamboo_get_args.java

OBJECTS:=$(RPC_SRC:.java=.class) $(OBJECTS)

all: $(OBJECTS) gateway_test # subdirs

subdirs:
	$(MAKE) -C fssim

clean:
	rm -f $(RPC_SRC) *.class gateway_prot_*.* *.exe \
	      gateway_prot.h gateway_test gateway_test.o
	$(MAKE) -C fssim clean

$(RPC_SRC): gateway_prot.x
	$(BAMBOO_HOME)/bin/run-java -jar $(BAMBOO_HOME)/lib/jrpcgen.jar -p tests.org.acplt.oncrpc.jrpcgen -noserver -nobackup -p bamboo.dht $<

ifeq (Windows_NT,$(OS))
#gateway_test: gateway_test.o gateway_prot_clnt.o gateway_prot_xdr.o
#	gcc -o $@ gateway_test.o gateway_prot_clnt.o gateway_prot_xdr.o -lrpc
gateway_test:
	echo not_for_windows > gateway_test
else
gateway_test: gateway_test.o gateway_prot_clnt.o gateway_prot_xdr.o
	gcc -o $@ gateway_test.o gateway_prot_clnt.o gateway_prot_xdr.o
endif

gateway_prot.h: gateway_prot.x
	rpcgen -h $< > $@

gateway_prot_clnt.c: gateway_prot.x
	rpcgen -l $< > $@

gateway_prot_xdr.c: gateway_prot.x
	rpcgen -c $< > $@

gateway_test.c: gateway_prot.h
gateway_prot_xdr.c: gateway_prot.h
gateway_prot_clnt.c: gateway_prot.h

%.o: %.c
	gcc -c $<

