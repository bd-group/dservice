#!/bin/bash

mkdir -p data
mkdir -p log
bin/redis-server conf/redis1.conf > log/r1.log &  echo $! > /tmp/mm.r1.pid
bin/redis-server conf/redis1b.conf > log/r1b.log &  echo $! >> /tmp/mm.r1.pid

bin/redis-sentinel conf/sentinel1.conf > log/s1.log &
echo $! > /tmp/mm.s1.pid
bin/redis-sentinel conf/sentinel2.conf > log/s2.log &
echo $! >> /tmp/mm.s1.pid
bin/redis-sentinel conf/sentinel3.conf > log/s3.log &
echo $! >> /tmp/mm.s1.pid
