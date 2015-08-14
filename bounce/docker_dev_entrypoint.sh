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
java -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.port=1101 \
    -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
    -cp /data/assets:/data/classes:/data/jar com.bouncestorage.bounce.Main \
    --properties /data/bounce.properties >& /var/log/bounce.log &

jstatd -J-Djava.security.policy=jstatd.all.policy -p 1099 &


child=$!
wait "$child"
