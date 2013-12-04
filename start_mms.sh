#!/bin/bash

LD_LIBRARY_PATH=lib/ CLASSPATH=lib/commons-pool-1.6.jar:lib/jetty-all-7.0.2.v100331.jar:lib/servlet-api-2.5.jar:lib/sigar.jar:lib/jedis-2.2.1.jar:build/iie.jar java iie.mm.server.MMServer -r 192.168.1.221 -p 20000 -hp 30303 -rr localhost -rp 6379 -blk 64000000 -prd 10 -sa "c;d" -stl "node36:26379;node37:26379;node38:26379;node39:26379"

