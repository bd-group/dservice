#!/bin/bash

echo "Stop servers ..."
killall redis-server

if [ "x$1" == "xall" ]; then
	echo "Stop sentinels ..."
	killall redis-sentinel
fi
