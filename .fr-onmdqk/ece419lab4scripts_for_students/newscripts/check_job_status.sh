#!/bin/bash
java -classpath ../src/lib/zookeeper-3.3.2.jar:../src/lib/log4j-1.2.15.jar:../src/. ClientDriver $1:$2 status $3
