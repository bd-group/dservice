##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#
# Time-stamp: <2013-05-02 17:30:31 macan>
#
# This is the makefile for HVFS project.
#
# Armed by EMACS.

GCC = gcc
ECHO = /bin/echo
# TODO: Make sure REMOVE self_test flag when release code
CFLAGS = -Wall -DNO_LINK -pg -g -O2 -DSELF_TEST
LDFLAGS = -Llib -lhvfs -lpthread -lrt

ifeq ($(JAVA_HOME),)
JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64
endif

HEADERS = common.h
DSERVICE = dservice
DEVMAP = devmap
DEVMAP_SO = lib$(DEVMAP).so
JTEST = Test
MSHOME = /home/macan/workspace/hive-0.10.0/src/build/dist/lib
LCHOME = /home/macan/workspace/lucene-4.2.1/build
METASTORE_API = $(MSHOME)/hive-metastore-0.10.0.jar
LUCENE_JAR = lib/lucene-core-4.2.1-SNAPSHOT.jar
LUCENE_TEST_JAR = $(LCHOME)/analysis/common/lucene-analyzers-common-4.2.1-SNAPSHOT.jar:$(LCHOME)/queries/lucene-queries-4.2.1-SNAPSHOT.jar:$(LCHOME)/sandbox/lucene-sandbox-4.2.1-SNAPSHOT.jar

THRIFT_JAR = $(MSHOME)/libthrift-0.9.0.jar:$(MSHOME)/libfb303-0.9.0.jar
#HADOOP_CORE = /home/macan/workspace/hadoop-1.0.3/hadoop-core-1.0.3.jar

METASTORE_RUNTIME = $(METASTORE_API):$(MSHOME)/commons-lang-2.4.jar:$(THRIFT_JAR):../$(LUCENE_JAR):$(LUCENE_TEST_JAR)

MSCLI_RUNTIME = $(METASTORE_RUNTIME):$(THRIFT_JAR)

CP = $(METASTORE_API):$(LUCENE_JAR):build/devmap.jar:$(LUCENE_TEST_JAR)

IIE = iie
MSCLI = mscli

OBJS = $(DSERVICE) $(DEVMAP_SO) $(JTEST).class

all: $(OBJS) $(IIE) $(MSCLI)	
	@$(ECHO) -e "Build OK."

$(DSERVICE): $(DSERVICE).c $(HEADERS)
	@$(ECHO) -e " " CC"\t" $@
	@$(GCC) $(CFLAGS) -Llib $(DSERVICE).c -o build/$(DSERVICE) $(LDFLAGS)

$(DEVMAP_SO): $(DEVMAP).c devmap/DevMap.java
	@javac -d build devmap/DevMap.java 
	@javah -d build devmap.DevMap 
	@$(ECHO) -e " " CC"\t" $@
	@$(GCC) -fPIC $(CFLAGS) -c $(DEVMAP).c -I $(JAVA_HOME)/include -I build -o \
		build/$(DEVMAP).o
	@$(GCC) build/$(DEVMAP).o -shared -o build/$(DEVMAP_SO) -Wl,-soname,devmap -lrt
	@$(ECHO) -e " " JAR"\t" devmap.jar
	@cd build ; jar cvf devmap.jar devmap/*.class

$(JTEST).class : $(JTEST).java $(IIE)
	@$(ECHO) -e " " JAVAC"\t" $(JTEST)
	@CLASSPATH=$(CP):build/iie.jar javac $(JTEST).java -d build

$(IIE): $(IIE)/index/lucene/*.java $(DEVMAP_SO)
	@$(ECHO) -e " " JAVAC"\t" $@
	@CLASSPATH=$(CP) javac -d build $(IIE)/index/lucene/*.java
	@$(ECHO) -e " " JAR"\t" devmap.jar
	@cd build; jar cvf iie.jar $(IIE)/index/lucene/*.class

$(MSCLI) : $(IIE)/metastore/*.java
	@$(ECHO) -e " " JAVAC"\t" $@
	@CLASSPATH=$(CP):$(MSCLI_RUNTIME) javac -d build $(IIE)/metastore/*.java

jtest: $(JTEST).class
	@cp lib/*.jar build/
	@cd build; LD_LIBRARY_PATH=. CLASSPATH=$(METASTORE_RUNTIME):$(CLASSPATH) java $(JTEST)

run: $(DSERVICE)
	@$(ECHO) -e "Run DService ..."
	@valgrind --leak-check=full build/dservice

runcli : $(MSCLI)
	@$(ECHO) -e "Run MetaStoreClient ..."
	@cd build; for f in /home/macan/workspace/hive-0.10.0/src/build/dist/lib/*.jar; do LIBS=$$LIBS:$$f; done; for f in /home/macan/workspace/hadoop-1.0.3/*.jar; do LIBS=$$LIBS:$$f; done; LD_LIBRARY_PATH=. CLASSPATH=$(METASTORE_RUNTIME):$(CLASSPATH):$(MSCLI_RUNTIME)$$LIBS java iie/metastore/MetaStoreClient

clean:
	-@rm -rf $(OBJS) *.o devmap_*.h *.class gmon.out *.jar build/*
