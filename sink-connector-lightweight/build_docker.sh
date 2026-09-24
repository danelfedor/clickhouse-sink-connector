#!/bin/bash
set -e

mvn clean install -DskipTests=true -Djavac.args="-g"
today_date=$(date +%F)

docker build . -t harbor.ivocap.com/qti/clickhouse_debezium_embedded:${today_date}-sqlserver --no-cache
docker push harbor.ivocap.com/qti/clickhouse_debezium_embedded:${today_date}-sqlserver
