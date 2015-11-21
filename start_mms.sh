#!/bin/bash

LD_PRELOAD=lib/liblmdb.so \
LD_LIBRARY_PATH=lib/:/home/macan/Downloads/OPENCV/opencv-2.4.9/build/lib/ \
CLASSPATH=lib/lmdbjni-all-99-master-20130507.185246-2.jar:lib/opencv-249.jar:lib/metadata-extractor-2.3.1.jar:lib/JOpenSurf.jar:lib/commons-math3-3.2.jar:lib/lire.jar:lib/lucene-core-4.2.1.jar:lib/lucene-analyzers-common-4.2.1.jar:lib/lucene-queries-4.2.1.jar:lib/commons-fileupload-1.3.1.jar:lib/commons-io-2.2.jar:lib/commons-pool2-2.1.jar:lib/jetty-server-7.6.17.v20150415.jar:lib/jetty-util-7.6.17.v20150415.jar:lib/jetty-http-7.6.17.v20150415.jar:lib/jetty-io-7.6.17.v20150415.jar:lib/jetty-continuation-7.6.17.v20150415.jar:lib/servlet-api-2.5.jar:lib/sigar.jar:lib/jedis-2.7.2.jar:build/iie.jar \
java iie.mm.server.MMServer \
	-server -Xms2g -Xmx2g \
        -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC \
        -XX:+UseParNewGC  -XX:ParallelGCThreads=8 \
        -XX:+UseConcMarkSweepGC -XX:+CMSScavengeBeforeRemark  -XX:+CMSParallelRemarkEnabled     \
        -XX:CMSInitiatingOccupancyFraction=80 \
        -XX:+UseCMSCompactAtFullCollection  -XX:CMSFullGCsBeforeCompaction=8 \
        -XX:SurvivorRatio=8 -XX:NewRatio=1 -XX:MaxTenuringThreshold=250 \
        -verbose:gc -Xloggc:/tmp/metastore/gc.log \
	-Djava.library.path=/home/macan/Downloads/OPENCV/opencv-2.4.9/build/lib \
	-p 10000 -hp 20202 -blk 67108864 -prd 10 -sa "a;b" \
	-stl "localhost:26379;localhost:26380;localhost:26381" \
	-sr localhost -ip .69. -wto 40 -rto 30 \
	-fXML conf/haarcascade_frontalface_alt.xml &> log/mms1.log &

LD_PRELOAD=lib/liblmdb.so \
LD_LIBRARY_PATH=lib/:~/Downloads/OPENCV/opencv-2.4.9/build/lib/ \
CLASSPATH=lib/lmdbjni-all-99-master-20130507.185246-2.jar:lib/opencv-249.jar:lib/metadata-extractor-2.3.1.jar:lib/JOpenSurf.jar:lib/commons-math3-3.2.jar:lib/lire.jar:lib/lucene-core-4.2.1.jar:lib/lucene-analyzers-common-4.2.1.jar:lib/lucene-queries-4.2.1.jar:lib/commons-fileupload-1.3.1.jar:lib/commons-io-2.2.jar:lib/commons-pool2-2.1.jar:lib/jetty-util-7.6.17.v20150415.jar:lib/jetty-server-7.6.17.v20150415.jar:lib/jetty-http-7.6.17.v20150415.jar:lib/jetty-io-7.6.17.v20150415.jar:lib/jetty-continuation-7.6.17.v20150415.jar:lib/servlet-api-2.5.jar:lib/sigar.jar:lib/jedis-2.7.2.jar:build/iie.jar \
java iie.mm.server.MMServer \
	-server -Xms2g -Xmx2g \
        -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC \
        -XX:+UseParNewGC  -XX:ParallelGCThreads=8 \
        -XX:+UseConcMarkSweepGC -XX:+CMSScavengeBeforeRemark  -XX:+CMSParallelRemarkEnabled     \
        -XX:CMSInitiatingOccupancyFraction=80 \
        -XX:+UseCMSCompactAtFullCollection  -XX:CMSFullGCsBeforeCompaction=8 \
        -XX:SurvivorRatio=8 -XX:NewRatio=1 -XX:MaxTenuringThreshold=250 \
        -verbose:gc -Xloggc:/tmp/metastore/gc.log \
	-p 20000 -hp 30303 -blk 67108864 -prd 10 -sa "c;d" \
	-stl "localhost:26379;localhost:26380;localhost:26381" \
	-sr localhost -ip .69. -wto 40 -rto 30 \
	-ssm -msize 3 &> log/mms2.log &

