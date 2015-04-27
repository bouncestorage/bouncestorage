#!/bin/bash

set -e

/etc/init.d/influxdb start
until [ echo >/dev/tcp/localhost/8086 ]; do
    service influxdb start || true
    sleep 5
done
curl -s -X POST 'http://localhost:8086/db?u=root&p=root' -d '{"name": "bounce"}'
curl -s -X POST 'http://localhost:8086/db/bounce/users?u=root&p=root' \
    -d '{"name":"bounce", "password":"bounce"}'
