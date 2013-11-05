#!/bin/bash

cd /home/metastore/sotstore;
lagent/lagent -f nodes -sc "cd sotstore/dservice; killall -9 dservice"
killall -9 dservice
