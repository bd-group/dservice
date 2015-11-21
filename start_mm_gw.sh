#!/bin/bash

CLASSPATH=lib/commons-pool2-2.1.jar:lib/servlet-api-2.5.jar:lib/jedis-2.7.2.jar:lib/jetty-server-7.6.17.v20150415.jar:lib/jetty-util-7.6.17.v20150415.jar:lib/jetty-continuation-7.6.17.v20150415.jar:lib/jetty-io-7.6.17.v20150415.jar:lib/jetty-http-7.6.17.v20150415.jar:build/iie.jar java iie.mm.tools.MMRestGateway -uri STL://localhost:26379
