#!/bin/bash

mvn clean install -DskipTests=true -Djavac.args="-g"
today_date=$(date +%F)

docker build . -t clickhouse_debezium_embedded:${today_date} --no-cache
docker tag clickhouse_debezium_embedded:${today_date} harbor.ivocap.com/qti/clickhouse_debezium_embedded:${today_date}
docker push harbor.ivocap.com/qti/clickhouse_debezium_embedded:${today_date}
