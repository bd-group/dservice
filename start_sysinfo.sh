#!/bin/bash

if [ x"$1" == "xserver" ]; then
	echo "Starting SysInfoStat Server now ..."
	CLASSPATH=build/iie.jar java iie.metastore.SysInfoStat -s &> log/siss.log &
else
	echo "Starting SysInfoStat Client now ..."
	CLASSPATH=build/iie.jar java iie.metastore.SysInfoStat -c -r FIXME_NODE &> /dev/null &
fi

