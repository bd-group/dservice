#!/bin/bash

echo "Stop servers ..."

kill `cat /tmp/mm.r1.pid`

if [ "x$1" == "xall" ]; then
	echo "Stop sentinels ..."
	kill `cat /tmp/mm.s1.pid` 
fi
