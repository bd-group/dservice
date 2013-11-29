#!/bin/bash

mkdir -p data
bin/redis-server conf/redis1.conf > r1.log &
