#!/bin/bash

kill `jps | grep MMServer | awk '{print $1}'`
