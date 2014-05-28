#!/bin/bash

kill `jps | grep MMObjectSearcher | awk '{print $1}'`
