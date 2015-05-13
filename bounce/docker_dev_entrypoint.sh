#!/bin/bash

set -e

function sigterm_handler() {
    echo "Stopping influxdb"
    service influxdb stop
    kill -TERM $child
}

trap "sigterm_handler" TERM

/data/setup_influxdb.sh
rm -Rf /data/jar
mkdir -p /data/jar
pushd /data/jar
jar xf /data/bounce.jar
popd
java -cp /data/assets:/data/classes:/data/jar com.bouncestorage.bounce.Main \
    --properties /data/bounce.properties >& /var/log/bounce.log &

child=$!
wait "$child"
