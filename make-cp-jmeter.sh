#!/bin/bash

JMETERCP="jmeter-cp"

gradle clean build -x check

(cd scheduler/scheduler-server/; gradle performanceTestJar)

rm -rf $JMETERCP/*

cp -r dist/lib/* $JMETERCP/

rm $JMETERCP/scheduler-server*

rm $JMETERCP/rm-server*

cp -r scheduler/scheduler-server/build/classes/main/* $JMETERCP/

cp -r scheduler/scheduler-server/build/classes/test/* $JMETERCP/

cp -r scheduler/scheduler-server/build/resources/test/* $JMETERCP/

cp -r rm/rm-server/build/classes/main/* $JMETERCP/

cp -r rm/rm-server/build/classes/test/* $JMETERCP/

cp -r rm/rm-server/build/resources/test/* $JMETERCP/

cp scheduler/scheduler-server/build/resources/jars/junit-performance-tests.jar $JMETERCP/

mkdir -p $JMETERCP/dist/lib

(cd scheduler/scheduler-server/; gradle performanceTest)
