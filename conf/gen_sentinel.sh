#!/bin/bash

if [ "x$1" == "x" ]; then
	SENS=3
else
	SENS=$1
fi

SPORT=26379
for i in `seq 1 $SENS`; do
	echo Create Sentinel Conf $i, port $SPORT
	cp sentinel.conf .tmp.gen
	sed -e "s/26379/$SPORT/g" .tmp.gen > sentinel$i.conf
	
	let SPORT+=1
done

rm -rf .tmp.gen
