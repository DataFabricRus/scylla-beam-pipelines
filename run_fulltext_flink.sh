#!/usr/bin/env bash

PIPELINE_JAR="${PIPELINE_JAR:-target/scylla-rdf-pipelines-0.0.1-SNAPSHOT.jar}"
PIPELINE_BATCH_SIZE="${PIPELINE_BATCH_SIZE:-500000}"
PIPELINE_IS_INITIAL_LOAD="${PIPELINE_IS_INITIAL_LOAD:-false}"

ELASTICSEARCH_HOST="${ELASTICSEARCH_HOST:-10.132.0.88}"
ELASTICSEARCH_BATCH_SIZE="${ELASTICSEARCH_BATCH_SIZE:-1000}"

java -cp ${PIPELINE_JAR} cc.datafabric.scylladb.pipelines.fulltextload.FullTextLoadPipeline \
    --runner=FlinkRunner \
    --batchSize=${PIPELINE_BATCH_SIZE} --isInitialLoad=${PIPELINE_IS_INITIAL_LOAD} \
    --elasticsearchHost=${ELASTICSEARCH_HOST} --elasticsearchBatchSize=${ELASTICSEARCH_BATCH_SIZE} $@
