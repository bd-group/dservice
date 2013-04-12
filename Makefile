##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#
# Time-stamp: <2013-04-12 16:42:57 macan>
#
# This is the makefile for HVFS project.
#
# Armed by EMACS.

GCC = gcc
ECHO = /bin/echo
CFLAGS = -Wall -DNO_LINK -pg -g -O2
LDFLAGS = -Llib -lhvfs -lpthread -lrt

HEADERS = common.h
DSERVICE = dservice
DEVMAP = devmap
DEVMAP_SO = lib$(DEVMAP).so
JTEST = Test

OBJS = $(DSERVICE) $(DEVMAP_SO) $(JTEST).class

all: $(OBJS)
	@$(ECHO) -e "Build OK."

$(DSERVICE): $(DSERVICE).c $(HEADERS)
	$(GCC) $(CFLAGS) -Llib $(DSERVICE).c -o $(DSERVICE) $(LDFLAGS)

$(DEVMAP_SO): $(DEVMAP).c devmap/DevMap.java
	javac devmap/DevMap.java
	javah devmap.DevMap
	$(GCC) -fPIC $(CFLAGS) -c $(DEVMAP).c -I $(JAVA_HOME)/include
	$(GCC) $(DEVMAP).o -shared -o $(DEVMAP_SO) -Wl,-soname,devmap -lrt

$(JTEST).class : $(JTEST).java
	javac $(JTEST).java

jtest: $(JTEST).class
	LD_LIBRARY_PATH=. java $(JTEST)

clean:
	-@rm -rf $(OBJS) *.o devmap_*.h *.class gmon.out
