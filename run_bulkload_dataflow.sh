#!/usr/bin/env bash

PIPELINE_JAR="${PIPELINE_JAR:-target/scylla-rdf-pipelines-0.0.1-SNAPSHOT.jar}"
PIPELINE_BATCH_SIZE="${PIPELINE_BATCH_SIZE:-500000}"

SCYLLA_HOSTS="${SCYLLA_HOSTS:-10.132.0.23,10.132.0.38}"
SCYLLA_PORT="${SCYLLA_PORT:-9042}"
SCYLLA_KEYSPACE="${SCYLLA_KEYSPACE:-triplestore}"
SCYLLA_MAX_REQUESTS_PER_CONNECTION="${SCYLLA_MAX_REQUESTS_PER_CONNECTION:-8192}"

java -jar ${PIPELINE_JAR} --runner=DataflowRunner --hosts=${SCYLLA_HOSTS} \
    --port=${SCYLLA_PORT} --keyspace=${SCYLLA_KEYSPACE} \
    --maxRequestsPerConnection=${SCYLLA_MAX_REQUESTS_PER_CONNECTION} \
    --batchSize=${PIPELINE_BATCH_SIZE} \
    --jobName=scylla-rdf-bulkload --project=core-datafabric --region=europe-west1 \
    --tempLocation=gs://datafabric-dataflow/temp --gcpTempLocation=gs://datafabric-dataflow/staging \
    --maxNumWorkers=20 --numWorkers=20 $@