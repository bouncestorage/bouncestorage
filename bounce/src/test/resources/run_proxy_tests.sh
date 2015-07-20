#!/bin/bash

set -o xtrace
set -o errexit
set -o nounset

function cleanup {
    if [ "$PROXY_PID" != "" ]; then
        kill $PROXY_PID
    fi
}

trap cleanup EXIT

PROXY_BIN="mvn -o exec:java $BOUNCE_OPTS -Dbounce.autoconfig-tiers=true"
stdbuf -oL -eL $PROXY_BIN -Dexec.args='--properties src/main/resources/bounce.properties' &
PROXY_PID=$!

export SKIP_PROXY=1
pushd ../swiftproxy
sleep 5

SKIP_TESTS="-e test.functional.tests:TestFile.testCopyFromHeader -e test.functional.tests:TestSlo.test_slo_copy_account -e test.functional.tests:TestSlo.test_slo_copy_the_manifest_account -e test.functional.tests:TestFile.testCopy -e test.functional.tests:TestFile.testCopyAccount -e test.functional.tests:TestFile.testCopyFromHeader404s -e test.functional.tests:TestFile.testNameLimit"
export SKIP_TESTS

src/test/resources/run-swift-tests.sh
exit $?
