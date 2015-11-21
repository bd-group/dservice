#!/bin/bash

if [ "x$1" != "x" ]; then
	LOG_PATH="-p $1"
fi

if [ "x$2" != "x" ]; then
	XSEC=$2
else
	XSEC=1
fi

while true; do
	LD_LIBRARY_PATH=lib/ CLASSPATH=lib/commons-pool2-2.1.jar:lib/jedis-2.7.2.jar:build/iie.jar java iie.mm.tools.RedisInfoMonitor -uri "STL://localhost:26379" $LOG_PATH; 
	sleep $XSEC;
done

