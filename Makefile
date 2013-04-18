##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#
# Time-stamp: <2013-04-17 15:32:07 macan>
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
JAVA_HOME=/usr/lib/jvm/java-6-openjdk
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

METASTORE_RUNTIME = $(METASTORE_API):$(MSHOME)/commons-lang-2.4.jar:$(MSHOME)/libthrift-0.9.0.jar:../$(LUCENE_JAR):$(LUCENE_TEST_JAR)
CP = $(METASTORE_API):$(LUCENE_JAR):build/devmap.jar:$(LUCENE_TEST_JAR)

IIE = iie

OBJS = $(DSERVICE) $(DEVMAP_SO) $(JTEST).class

all: $(OBJS) $(IIE)
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

jtest: $(JTEST).class
	@cp lib/*.jar build/
	@cd build; LD_LIBRARY_PATH=. CLASSPATH=$(METASTORE_RUNTIME):$(CLASSPATH) java $(JTEST)

clean:
	-@rm -rf $(OBJS) *.o devmap_*.h *.class gmon.out *.jar build/*
