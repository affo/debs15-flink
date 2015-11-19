#!/bin/bash
start-local-streaming.sh
flink run "$@" debs15-flink.jar &

echo "Connect to http://localhost:48081/#/jobmanager/stdout"
echo "to see the standard output, or watch here."

sleep 2

# To see logs
tail -f $FLINK_HOME/log/flink-*.out
