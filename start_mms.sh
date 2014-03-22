#!/bin/bash

#LD_LIBRARY_PATH=lib/ CLASSPATH=lib/commons-pool-1.6.jar:lib/jetty-all-7.0.2.v20100331.jar:lib/servlet-api-2.5.jar:lib/sigar.jar:lib/jedis-2.2.1.jar:build/iie.jar java iie.mm.server.MMServer -p 20000 -hp 30303 -blk 64000000 -prd 10 -sa "c;d" -stl "node36:26379;node37:26379;node38:26379;node39:26379"
LD_LIBRARY_PATH=lib/ CLASSPATH=lib/commons-pool-1.6.jar:lib/jetty-all-7.0.2.v20100331.jar:lib/servlet-api-2.5.jar:lib/sigar.jar:lib/jedis-2.2.1.jar:build/iie.jar java iie.mm.server.MMServer -p 10000 -hp 20202 -blk 64000000 -prd 10 -sa "a;b" -stl "localhost:26379;localhost:26380;localhost:26381" &> log/mms1.log &
LD_LIBRARY_PATH=lib/ CLASSPATH=lib/commons-pool-1.6.jar:lib/jetty-all-7.0.2.v20100331.jar:lib/servlet-api-2.5.jar:lib/sigar.jar:lib/jedis-2.2.1.jar:build/iie.jar java iie.mm.server.MMServer -p 20000 -hp 30303 -blk 64000000 -prd 10 -sa "c;d" -stl "localhost:26379;localhost:26380;localhost:26381" &> log/mms2.log &

