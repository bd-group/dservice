#!/bin/bash

CLASSPATH=lib/servlet-api-2.5.jar:lib/jetty-all-7.0.2.v20100331.jar:build/iie.jar java iie.monitor.MonitorServer 33333 conf/monitor.conf sotstore/sotstore/reports > log/monitor.log & 
