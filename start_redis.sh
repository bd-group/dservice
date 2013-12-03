#!/bin/bash

mkdir -p data
mkdir -p log
bin/redis-server conf/redis1.conf > log/r1.log &
bin/redis-server conf/redis1b.conf > log/r1b.log &

bin/redis-sentinel conf/sentinel1.conf > log/s1.log &
bin/redis-sentinel conf/sentinel2.conf > log/s2.log &
bin/redis-sentinel conf/sentinel3.conf > log/s3.log &
