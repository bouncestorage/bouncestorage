#!/bin/bash

set -e

function sigterm_handler() {
    echo "Stopping influxdb"
    service influxdb stop
    kill -TERM $child
}

trap "sigterm_handler" TERM

service influxdb start
java -jar ./bounce.jar --properties bounce.properties >& /var/log/bounce.log &

child=$!
wait "$child"
