#!/bin/bash
start-local-streaming.sh
echo "Connect to http://localhost:48081/#/jobmanager/stdout"
echo "to see the standard output"
flink run "$@" debs15-flink.jar
/bin/bash # stay attached
