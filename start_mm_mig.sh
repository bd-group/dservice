#!/bin/bash

LD_LIBRARY_PATH=lib/ CLASSPATH=lib/lucene-core-4.2.1.jar:lib/lucene-analyzers-common-4.2.1.jar:lib/lucene-queries-4.2.1.jar:lib/commons-fileupload-1.3.1.jar:lib/commons-io-2.2.jar:lib/commons-pool2-2.1.jar:lib/jetty-server-7.6.17.v20150415.jar:lib/jetty-util-7.6.17.v20150415.jar:lib/jetty-http-7.6.17.v20150415.jar:lib/jetty-io-7.6.17.v20150415.jar:lib/jetty-continuation-7.6.17.v20150415.jar:lib/servlet-api-2.5.jar:lib/sigar.jar:lib/jedis-2.7.2.jar:lib/lmdbjni-all-99-master-20130507.185246-2.jar:build/iie.jar java iie.mm.tools.Redis2LMDB -uri "STL://localhost:26379" -daystr $1

