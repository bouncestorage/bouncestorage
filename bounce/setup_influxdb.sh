#!/bin/bash

set -e

setup_file=/data/setup

service influxdb start

if [ -e $setup_file ]; then
	exit 0
fi

until [ echo >/dev/tcp/localhost/8086 ]; do
    service influxdb start || true
    sleep 5
done
curl -s -X POST 'http://localhost:8086/db?u=root&p=root' -d '{"name": "bounce"}'
curl -s -X POST 'http://localhost:8086/db/bounce/users?u=root&p=root' \
    -d '{"name":"bounce", "password":"bounce"}'

touch $setup_file
