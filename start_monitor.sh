#!/bin/bash

CLASSPATH=lib/servlet-api-2.5.jar:lib/jetty-server-7.6.17.v20150415.jar:lib/jetty-util-7.6.17.v20150415.jar:lib/jetty-http-7.6.17.v20150415.jar:lib/jetty-io-7.6.17.v20150415.jar:lib/jetty-continuation-7.6.17.v20150415.jar:build/iie.jar java iie.monitor.MonitorServer 33333 conf/monitor.conf sotstore/sotstore/reports > log/monitor.log & 
