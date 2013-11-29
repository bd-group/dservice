##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#
# Time-stamp: <2013-10-18 14:34:59 macan>
#
# This is the makefile for HVFS project.
#
# Armed by EMACS.

GCC = gcc
ECHO = /bin/echo
# TODO: Make sure REMOVE self_test flag when release code
CFLAGS = -Wall -DNO_LINK -pg -g -O2 -DSELF_TEST_
LDFLAGS = -Llib -lhvfs -lpthread -lrt

include Makefile.profile

HEADERS = common.h jsmn.h
DSERVICE = dservice
DEVMAP = devmap
DEVMAP_SO = lib$(DEVMAP).so
JTEST = Test

LUCENE_JAR = $(LCHOME)/lucene-core-4.2.1.jar
LUCENE_TEST_JAR = $(LCHOME)/lucene-analyzers-common-4.2.1.jar:$(LCHOME)/lucene-queries-4.2.1.jar:$(LCHOME)/lucene-sandbox-4.2.1.jar

THRIFT_JAR = $(MSHOME)/libthrift-0.9.0.jar:$(MSHOME)/libfb303-0.9.0.jar

METASTORE_API = $(MSHOME)/hive-metastore-0.10.0.jar:$(THRIFT_JAR)
METASTORE_RUNTIME = $(METASTORE_API):$(MSHOME)/commons-lang-2.4.jar:$(THRIFT_JAR):$(LUCENE_JAR):$(LUCENE_TEST_JAR)

MSCLI_RUNTIME = $(METASTORE_RUNTIME)

MM_CP = $(shell pwd)/lib/jedis-2.2.1.jar:$(shell pwd)/lib/junixsocket-1.3.jar:$(shell pwd)/lib/sigar.jar:$(shell pwd)/lib/jetty-all-7.0.2.v20100331.jar:$(shell pwd)/lib/servlet-api-2.5.jar:$(shell pwd)/lib/commons-pool-1.6.jar

MM_JARS = $(shell pwd)/lib/jedis-2.2.1.jar $(shell pwd)/lib/junixsocket-1.3.jar $(shell pwd)/lib/sigar.jar $(shell pwd)/lib/jetty-all-7.0.2.v20100331.jar $(shell pwd)/lib/servlet-api-2.5.jar $(shell pwd)/lib/commons-pool-1.6.jar

CP = $(METASTORE_API):$(LUCENE_JAR):build/devmap.jar:$(LUCENE_TEST_JAR):$(MM_CP)

IIE = iie
MSCLI = mscli

OBJS = $(DSERVICE) $(DEVMAP_SO) $(JTEST).class

all: $(OBJS) $(IIE) $(MSCLI)	
	@$(ECHO) -e "Build OK."

$(DSERVICE): $(DSERVICE).c $(HEADERS)
	@$(ECHO) -e " " CC"\t" $@
	@mkdir -p build
	@$(GCC) $(CFLAGS) -Llib $(DSERVICE).c jsmn.c -o build/$(DSERVICE) $(LDFLAGS)

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
	@CLASSPATH=$(CP) javac -d build $(IIE)/mm/client/*.java
	@CLASSPATH=$(CP) javac -d build $(IIE)/mm/server/*.java
	@$(ECHO) -e " " JAR"\t" devmap.jar
	@cd build; jar cvf iie.jar $(IIE)/index/lucene/*.class $(IIE)/metastore/*.class $(IIE)/mm/client/*.class $(IIE)/mm/server/*.class $(MM_JARS)

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

clean:
	-@rm -rf $(OBJS) *.o devmap_*.h *.class gmon.out *.jar build/*
