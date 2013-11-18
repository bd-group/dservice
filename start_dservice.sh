#!/bin/bash

cd /home/metastore/sotstore;
lagent/lagent -f nodes -sc "cd sotstore/dservice; build/dservice -r FIXME_NODE -p FIXME_PORT -x -o 10 > ds.log &"
