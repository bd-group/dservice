#!/bin/bash

LD_PRELOAD=lib/liblmdb.so LD_LIBRARY_PATH=lib/:~/Downloads/OPENCV/opencv-2.4.9/build/lib/ CLASSPATH=lib/lmdbjni-all-99-master-20130507.185246-2.jar:lib/opencv-249.jar:lib/metadata-extractor-2.3.1.jar:lib/JOpenSurf.jar:lib/commons-math3-3.2.jar:lib/lire.jar:lib/lucene-core-4.2.1.jar:lib/lucene-analyzers-common-4.2.1.jar:lib/lucene-queries-4.2.1.jar:lib/commons-fileupload-1.3.1.jar:lib/commons-io-2.2.jar:lib/commons-pool2-2.0.jar:lib/jetty-all-7.0.2.v20100331.jar:lib/servlet-api-2.5.jar:lib/sigar.jar:lib/jedis-2.5.1.jar:build/iie.jar java iie.mm.server.MMServer -server -p 20000 -hp 30303 -blk 64000000 -prd 10 -sa "c;d" -stl "localhost:26379;localhost:26380;localhost:26381" -sr localhost -ip .69. -wto 40 -rto 30 -lmdb "." -ssm &> log/mms.log &

