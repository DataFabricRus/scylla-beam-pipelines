#!/usr/bin/env bash

SCYLLA_RDF_PIPELINES_JAR="${SCYLLA_RDF_PIPELINES_JAR:-target/scylladb-pipelines-1.0-SNAPSHOT.jar}"
SCYLLA_RDF_STORAGE_HOSTS="${SCYLLA_RDF_STORAGE_HOSTS:-localhost}"
SCYLLA_RDF_STORAGE_PORT="${SCYLLA_RDF_STORAGE_HOSTS:-9042}"
SCYLLA_RDF_STORAGE_KEYSPACE="${SCYLLA_RDF_STORAGE_KEYSPACE:-triplestore}"

java -jar ${SCYLLA_RDF_PIPELINES_JAR} --runner=DataflowRunner --hosts=${SCYLLA_RDF_STORAGE_HOSTS} \
    --port=${SCYLLA_RDF_STORAGE_PORT} --keyspace=${SCYLLA_RDF_STORAGE_KEYSPACE} --batchSize=500000 \
    --jobName=scylladb-bulkload --project=core-datafabric --region=europe-west1 \
    --tempLocation=gs://datafabric-dataflow/temp --gcpTempLocation=gs://datafabric-dataflow/staging \
    --maxNumWorkers=1 --numWorkers=1 $@