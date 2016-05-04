#!/bin/bash
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

java -jar telco-ui/target/telcoui.jar &
java -jar maprdb-consumer/target/maprdb-consumer-1.0-SNAPSHOT.jar &
java -jar spark-consumer/target/spark-consumer-1.0-SNAPSHOT.jar &
sleep 10
java -jar universe/target/universe-1.0-SNAPSHOT.jar &


echo Started jobs:
jobs -pr

wait
