#!/bin/bash

if [ "x$DSHOME" == "x" ]; then
    DSHOME=`pwd`/..
    echo "Please set env DSHOME as top level workspace, default '$DSHOME'."
fi
if [ "x$MSHOME" == "x" ]; then
    MSHOME=$DSHOME/hive-0.10.0/src/build/dist/lib
    echo "Please set env MSHOME as metastore dist lib, default '$MSHOME'."
fi

if [ "x$LCHOME" == "x" ]; then
    LCHOME=$DSHOME/dservice/lib
    echo "Please set env LCHOME as lucene home, default as '$LCHOME'."
fi

if [ "x$HADOOP_HOME" == "x" ]; then
    HADOOP_HOME=$DSHOME/hadoop-1.0.3
    echo "Please set env HADOOP_HOME as hadoop home, default as '$HADOOP_HOME'."
fi

cd build
for f in $MSHOME/*.jar; do 
    LIBS=$LIBS:$f; 
done
for f in $HADOOP_HOME/*.jar; do 
    LIBS=$LIBS:$f; done

LD_LIBRARY_PATH=. CLASSPATH=$CLASSPATH:$LIBS:iie.jar java iie/metastore/MetaStoreClient "$@"
