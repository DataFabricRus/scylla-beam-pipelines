#!/usr/bin/env bash

SCYLLA_RDF_PIPELINES_JAR="${SCYLLA_RDF_PIPELINES_JAR:-target/scylladb-pipelines-0.0.1-SNAPSHOT.jar}"
SCYLLA_RDF_STORAGE_HOSTS="${SCYLLA_RDF_STORAGE_HOSTS:-localhost}"
SCYLLA_RDF_STORAGE_PORT="${SCYLLA_RDF_STORAGE_PORT:-9042}"
SCYLLA_RDF_STORAGE_KEYSPACE="${SCYLLA_RDF_STORAGE_KEYSPACE:-triplestore}"
SCYLLA_RDF_PIPELINE_BATCH_SIZE="${SCYLLA_RDF_PIPELINE_BATCH_SIZE:-10000}"
ELASTICSEARCH_HOST="${ELASTICSEARCH_HOST:-localhost}"
ELASTICSEARCH_BATCH_SIZE="${ELASTICSEARCH_BATCH_SIZE:-100}"

java -jar ${SCYLLA_RDF_PIPELINES_JAR} --runner=DataflowRunner --hosts=${SCYLLA_RDF_STORAGE_HOSTS} \
    --port=${SCYLLA_RDF_STORAGE_PORT} --keyspace=${SCYLLA_RDF_STORAGE_KEYSPACE} \
    --batchSize=${SCYLLA_RDF_PIPELINE_BATCH_SIZE} \
    --elasticsearchHost=${ELASTICSEARCH_HOST} --elasticsearchBatchSize=${ELASTICSEARCH_BATCH_SIZE} \
    --jobName=scylladb-bulkload --project=core-datafabric --region=europe-west1 \
    --tempLocation=gs://datafabric-dataflow/temp --gcpTempLocation=gs://datafabric-dataflow/staging \
    --maxNumWorkers=1 --numWorkers=1 $@