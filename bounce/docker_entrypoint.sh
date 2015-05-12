#!/bin/bash

set -e

function sigterm_handler() {
    echo "Stopping influxdb"
    service influxdb stop
    kill -TERM $child
}

trap "sigterm_handler" TERM

/data/setup_influxdb.sh
/data/setup_keystore.sh
java -jar /data/bounce.jar --properties /data/bounce.properties >& /var/log/bounce.log &

child=$!
wait "$child"
