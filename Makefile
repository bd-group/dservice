##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#
# Time-stamp: <2015-09-18 17:17:27 macan>
#
# This is the makefile for HVFS project.
#
# Armed by EMACS.

GCC = gcc
ECHO = /bin/echo
MAKE = make
SHELL := /bin/bash

GIT = env git
GIT_SHA = `$(GIT) rev-parse HEAD`
GIT_DIRTY = `$(GIT) diff --shortstat 2> /dev/null | tail -n1`

INOTIFY=inotify-tools-3.14

COMPILE_DATE = `date`
COMPILE_HOST = `hostname`

# TODO: Make sure REMOVE self_test flag when release code
CFLAGS = -Wall -DNO_LINK -pg -g -O2 -DCOMPILE_DATE="\"$(COMPILE_DATE)\"" -DCOMPILE_HOST="\"$(COMPILE_HOST)\"" -DGIT_SHA="\"$(GIT_SHA)\"" -DSELF_TEST_
LDFLAGS = -Llib -lhvfs -lpthread -lrt -pg -g

include Makefile.profile

HEADERS = common.h jsmn.h
DSERVICE = dservice
DEVMAP = devmap
DEVMAP_SO = lib$(DEVMAP).so
JTEST = Test
WATCHER = watcher

LUCENE_JAR = $(LCHOME)/lucene-core-4.2.1.jar
LUCENE_TEST_JAR = $(LCHOME)/lucene-analyzers-common-4.2.1.jar:$(LCHOME)/lucene-queries-4.2.1.jar:$(LCHOME)/lucene-sandbox-4.2.1.jar:$(LCHOME)/lucene-queryparser-4.2.1.jar

THRIFT_JAR = $(MSHOME)/libthrift-0.9.0.jar:$(MSHOME)/libfb303-0.9.0.jar

METASTORE_API = $(HADOOP_HOME)/hadoop-core-1.0.3.jar:$(MSHOME)/hive-metastore-0.10.0.jar:$(MSHOME)/hive-common-0.10.0.jar:$(MSHOME)/metamorphosis-client-1.4.4.jar:$(MSHOME)/metamorphosis-commons-1.4.4.jar:$(THRIFT_JAR)
METASTORE_RUNTIME = $(METASTORE_API):$(MSHOME)/commons-lang-2.4.jar:$(THRIFT_JAR):$(LUCENE_JAR):$(LUCENE_TEST_JAR)

MSCLI_RUNTIME = $(METASTORE_RUNTIME)

LMDB=$(shell pwd)/lib/lmdbjni-all-99-master-20130507.185246-2.jar
#LMDB=$(shell pwd)/lib/lmdbjni-0.4.0.jar:$(shell pwd)/lib/lmdbjni-linux64-0.4.0.jar

MM_CP = $(shell pwd)/lib/jedis-2.7.2.jar:$(shell pwd)/lib/junixsocket-1.3.jar:$(shell pwd)/lib/sigar.jar:$(shell pwd)/lib/jetty-all-7.0.2.v20100331.jar:$(shell pwd)/lib/jetty-server-7.6.17.v20150415.jar:$(shell pwd)/lib/jetty-util-7.6.17.v20150415.jar:$(shell pwd)/lib/servlet-api-2.5.jar:$(shell pwd)/lib/commons-pool2-2.1.jar:$(shell pwd)/lib/commons-io-2.2.jar:$(shell pwd)/lib/commons-fileupload-1.3.1.jar:$(shell pwd)/lib/lire.jar:$(shell pwd)/lib/commons-math3-3.2.jar:$(shell pwd)/lib/JOpenSurf.jar:$(shell pwd)/lib/metadata-extractor-2.3.1.jar:$(shell pwd)/lib/opencv-249.jar:$(LMDB)

CP = $(METASTORE_API):$(LUCENE_JAR):build/devmap.jar:$(LUCENE_TEST_JAR):$(MM_CP):build/:lib/fastjson-1.1.39.jar

MMCC = build/libmmcc.so
MMCA = build/libmmcc.a
MMHC = build/libmmhc.so
MMFS = build/libmmfs.so

DEMO = build/demo

IIE = iie
MSCLI = mscli
MFS = mfs

OBJS = $(DSERVICE) $(DEVMAP_SO) $(JTEST).class $(WATCHER)

all: $(OBJS) $(IIE) $(MSCLI) GEN_VERSION_FILE
	@$(ECHO) -e "Build OK."

mmcc : DEPEND $(MMCC) 

mmhc : DEPEND $(MMHC)

DEPEND : 
	@$(ECHO) -e " " MK Depends
	@if [ ! -d $(REDIS) ]; then tar zxvf $(REDIS).tar.gz; fi
	@$(MAKE) --no-print-directory -C $(REDIS)
	@rm -rf bin/*
	@mkdir -p bin
	@cp -rpf $(REDIS)/src/redis-server bin/
	@cp -rpf $(REDIS)/src/redis-cli bin/
	@cp -rpf $(REDIS)/src/redis-sentinel bin/
	@$(MAKE) --no-print-directory -C hiredis
	@rm -rf lib/libhiredis*
	@cp -rpf hiredis/libhiredis.so lib/
	@cd lib; ln -s libhiredis.so libhiredis.so.0.11
	@if [ ! -d $(LOCAL_DB) ]; then echo "No Local DB exists!"; fi
	@$(MAKE) --no-print-directory -C $(LOCAL_DB)
	@cp -rpf $(LOCAL_DB)/liblmdb.so lib/

GEN_VERSION_FILE :
	@$(ECHO) -e " " GEN .VERSION
	@echo $(GIT_SHA) > .VERSION
	@echo DIRTY: $(GIT_DIRTY) >> .VERSION
	@echo $(COMPILE_HOST) @ $(COMPILE_DATE) >> .VERSION

$(MMCC) : iie/mm/cclient/client.c iie/mm/cclient/clientapi.c iie/mm/cclient/rpool.c lib/libhvfs.a
	@$(ECHO) -e " " CC"\t" $@
	@$(GCC) -fPIC $(CFLAGS) -Llib -Ilib -Iiie/mm/cclient -Ihiredis -c iie/mm/cclient/client.c -o build/client.o
	@$(GCC) -fPIC $(CFLAGS) -Llib -Ilib -Iiie/mm/cclient -Ihiredis -c iie/mm/cclient/clientapi.c -o build/clientapi.o
	@$(GCC) -fPIC $(CFLAGS) -Llib -Ilib -Iiie/mm/cclient -Ihiredis -c iie/mm/cclient/rpool.c -o build/rpool.o
	@$(AR) rcs $(MMCA) build/client.o build/clientapi.o build/rpool.o
	@$(GCC) -Llib build/client.o build/clientapi.o build/rpool.o -shared -o $(MMCC) -Wl,-soname,libmmcc.so -lhiredis -lrt

version_hdr :
	@$(shell sh -c './mkversionhdr.sh')

$(MMFS) : $(MMCC) iie/mm/fuse/mmfs_ll.c iie/mm/fuse/mmfs.c lib/libhvfs.a version_hdr
	@$(ECHO) -e " " CC"\t" $@
	@$(GCC) -fPIC $(CFLAGS) -D_FILE_OFFSET_BITS=64 -D_REENTRANT -Llib -Ihiredis -Ilib -Iiie/mm/fuse -Iiie/mm/cclient -c iie/mm/fuse/mmfs.c -o build/mmfs.o
	@$(GCC) -fPIC $(CFLAGS) -D_FILE_OFFSET_BITS=64 -D_REENTRANT -Llib -Ihiredis -Ilib -Iiie/mm/fuse -Iiie/mm/cclient -c iie/mm/fuse/mmfs_ll.c -o build/mmfs_ll.o
	@$(GCC) -Llib -Lbuild build/mmfs.o build/mmfs_ll.o -shared -o $(MMFS) -Wl,-soname,libmmfs.so -lmmcc -lhiredis -lrt -lhvfs -lpthread

$(MFS) : $(MMFS) iie/mm/fuse/mmfs_fuse0.c iie/mm/fuse/mmfs_fuse1.c iie/mm/fuse/mmfs_mkfs.c lib/libhvfs.a iie/mm/fuse/version.h
	@$(GCC) -fPIC $(CFLAGS) -D_FILE_OFFSET_BITS=64 -D_REENTRANT -Llib -Ihiredis -Ilib -Iiie/mm/fuse -Iiie/mm/cclient -c iie/mm/fuse/mmfs_mkfs.c -o build/mmfs_mkfs.o
	@$(GCC) -fPIC $(CFLAGS) -D_FILE_OFFSET_BITS=64 -D_REENTRANT -Llib -Ihiredis -Ilib -Iiie/mm/fuse -Iiie/mm/cclient -c iie/mm/fuse/mmfs_fuse0.c -o build/mmfs_fuse0.o
	@$(GCC) -fPIC $(CFLAGS) -D_FILE_OFFSET_BITS=64 -D_REENTRANT -Llib -Ihiredis -Ilib -Iiie/mm/fuse -Iiie/mm/cclient -c iie/mm/fuse/mmfs_fuse1.c -o build/mmfs_fuse1.o
	@$(GCC) -Llib -Lbuild build/mmfs_mkfs.o build/mmfs_ll.o -o build/mkfs.mmfs -lmmcc -lhiredis -lrt -lhvfs -lpthread
	@$(GCC) -Llib -Lbuild build/mmfs_fuse0.o build/mmfs_ll.o -o build/mmfs_test -lmmcc -lhiredis -lrt -lhvfs -lpthread
	@$(GCC) -Llib -Lbuild build/mmfs_fuse1.o build/mmfs_ll.o -o build/mmfs_v1 -lmmfs -lfuse -lmmcc -lhiredis -lrt -lhvfs -lpthread
	@$(ECHO) done

$(MMHC) : iie/mm/hclient/hclient.c
	@$(ECHO) -e " " CC"\t" $@
	@$(GCC) -fPIC $(CFLAGS) -Llib -Ilib -Iiie/mm/hclient -c iie/mm/hclient/hclient.c -o build/hclient.o
	@$(GCC) -Llib build/hclient.o -shared -o $(MMHC) -Wl,-soname,libmmhc.so -lhiredis -lrt

demo : $(DEMO)

$(DEMO) : iie/mm/cclient/demo.c
	#@$(GCC) $(CFLAGS) -Ilib/ -Iiie/mm/cclient/ iie/mm/cclient/demo.c -o build/demo -Lbuild/ -Llib/ -lmmcc -lhiredis -lpthread
	@$(GCC) $(CFLAGS) -Ilib/ -Iiie/mm/cclient/ build/libmmcc.a iie/mm/cclient/demo.c -o build/demo -Lbuild/ -Llib/ -lmmcc -lhiredis -lpthread

$(DSERVICE): $(DSERVICE).c $(HEADERS)
	@$(ECHO) -e " " CC"\t" $@
	@mkdir -p build
	@$(GCC) $(CFLAGS) -Llib $(DSERVICE).c jsmn.c -o build/$(DSERVICE) $(LDFLAGS)

INOTIFY_DEPEND : 
	@$(ECHO) -e " " MK INOTIFY
	@if [ ! -d inotify-tools-3.14 ]; then tar zxvf inotify-tools-3.14.tar.gz; fi
	@$(MAKE) --no-print-directory -C inotify-tools-3.14

$(WATCHER): INOTIFY_DEPEND $(WATCHER).c
	@$(ECHO) -e " " CC"\t" $@
	@mkdir -p build
	@$(GCC) -DHVFS_TRACING $(CFLAGS) -Llib -I$(INOTIFY)/libinotifytools/src/ $(WATCHER).c $(INOTIFY)/libinotifytools/src/.libs/libinotifytools.a -o build/$(WATCHER) $(LDFLAGS) -lhvfs

$(DEVMAP_SO): $(DEVMAP).c devmap/DevMap.java
	@javac -d build devmap/DevMap.java 
	@javah -d build -classpath build devmap.DevMap 
	@$(ECHO) -e " " CC"\t" $@
	@$(GCC) -fPIC $(CFLAGS) -c $(DEVMAP).c -I $(JAVA_HOME)/include -I $(JAVA_HOME)/include/linux -I build -o \
		build/$(DEVMAP).o
	@$(GCC) build/$(DEVMAP).o -shared -o build/$(DEVMAP_SO) -Wl,-soname,devmap -lrt
	@$(ECHO) -e " " JAR"\t" devmap.jar
	@cd build ; jar cvf devmap.jar devmap/*.class

$(JTEST).class : $(JTEST).java $(IIE)
	@$(ECHO) -e " " JAVAC"\t" $(JTEST)
	@CLASSPATH=$(CP):build/iie.jar javac $(JTEST).java -d build

$(IIE): $(IIE)/index/lucene/*.java $(DEVMAP_SO) $(MSCLI)
	@$(ECHO) -e " " JAVAC"\t" $@
	@CLASSPATH=$(CP) javac -d build $(IIE)/index/lucene/*.java
	@CLASSPATH=$(CP) javac -d build $(IIE)/metastore/*.java
	@if [ -d $(IIE)/mm/common ]; then CLASSPATH=$(CP) javac -d build $(IIE)/mm/common/*.java; fi
	@CLASSPATH=$(CP) javac -d build $(IIE)/mm/client/*.java
	@CLASSPATH=$(CP) javac -d build $(IIE)/mm/tools/MM2SSMigrater.java
	@CLASSPATH=$(CP) javac -d build $(IIE)/mm/server/*.java
	@CLASSPATH=$(CP) javac -d build $(IIE)/mm/tools/*.java
	@CLASSPATH=$(CP) javac -d build $(IIE)/monitor/*.java
	@$(ECHO) -e " " JAR"\t" iie.jar
	@cd build; jar cvf iie.jar $(IIE)/index/lucene/*.class $(IIE)/metastore/*.class $(IIE)/mm/common/*.class $(IIE)/mm/client/*.class $(IIE)/mm/server/*.class $(IIE)/monitor/*.class $(IIE)/mm/tools/*.class; rm -rf $(IIE)/metastore;

$(MSCLI) : $(IIE)/metastore/*.java
	@$(ECHO) -e " " JAVAC"\t" $@
	@CLASSPATH=$(CP):$(MSCLI_RUNTIME) javac -d build $(IIE)/metastore/*.java

jtest: $(JTEST).class
	#@cp lib/*.jar build/
	@cd build; for f in $(MSHOME)/*.jar; do LIBS=$$LIBS:$$f; done; for f in $(HADOOP_HOME)/*.jar; do LIBS=$$LIBS:$$f; done; LD_LIBRARY_PATH=. CLASSPATH=$(METASTORE_RUNTIME):$(CLASSPATH):$(MSCLI_RUNTIME)$$LIBS java $(JTEST)

run: $(DSERVICE)
	@$(ECHO) -e "Run DService ..."
	@valgrind --leak-check=full build/dservice

runcli : $(MSCLI)
	@$(ECHO) -e "Run MetaStoreClient ..."
	@cd build; for f in $(MSHOME)/*.jar; do LIBS=$$LIBS:$$f; done; for f in $(HADOOP_HOME)/*.jar; do LIBS=$$LIBS:$$f; done; LD_LIBRARY_PATH=. CLASSPATH=$(METASTORE_RUNTIME):$(CLASSPATH):$(MSCLI_RUNTIME)$$LIBS java iie/metastore/MetaStoreClient

depend_clean:
	@$(MAKE) --no-print-directory -C $(REDIS) clean
	@$(MAKE) --no-print-directory -C hiredis clean
	@$(MAKE) --no-print-directory -C inotify-tools-3.14 clean

clean: depend_clean
	-@rm -rf $(OBJS) *.o devmap_*.h *.class gmon.out *.jar build/*
	-@rm -rf bin/*
	-@rm -rf lib/libhiredis*
