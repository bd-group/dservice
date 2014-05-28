#!/bin/sh

MYSERVICE_PID=`/sbin/pidof /home/metastore/sotstore/dservice/build/dservice`

check_myservice() {
        if [ -z $MYSERVICE_PID ];then
		echo `date` FAIL
                cd /home/metastore/sotstore/dservice; build/dservice -r FIXME_NODE -p 20404 -x -o 30 -R 2 -D 2 -S -M 7 > ds.log &
	else
		echo `date` OK
        fi
}

check_myservice
