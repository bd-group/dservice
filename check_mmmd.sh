#!/bin/bash

DATE=`date "+%Y-%m-%d %H:%M"`
echo Check begins at $DATE

#LD_LIBRARY_PATH=lib/ CLASSPATH=lib/commons-pool-1.6.jar:lib/jedis-2.2.1.jar:build/iie.jar java iie.mm.tools.MMRepChecker -rr localhost -rp 30999 -nsd &> "log/repcheck.${DATE}.log"
LD_LIBRARY_PATH=lib/ CLASSPATH=lib/commons-pool-1.6.jar:lib/jedis-2.2.1.jar:build/iie.jar java iie.mm.tools.MMRepChecker -rr localhost -rp 6379 -nsd &> "log/repcheck.${DATE}.log"

