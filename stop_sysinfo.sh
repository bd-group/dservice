#!/bin/bash

cd /home/metastore/sotstore;
#lagent/lagent -f nodes -sc "kill `jps | grep MMServer | awk '{print \$1}'`"
lagent/lagent -f nodes -sc 'kill `jps | grep SysInfoStat | awk "{print \$1}"`'

