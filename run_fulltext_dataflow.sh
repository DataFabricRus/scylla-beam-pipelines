#!/usr/bin/env bash

PIPELINE_JAR="${PIPELINE_JAR:-target/scylla-rdf-pipelines-0.0.1-SNAPSHOT.jar}"
PIPELINE_BATCH_SIZE="${PIPELINE_BATCH_SIZE:-500000}"
PIPELINE_IS_INITIAL_LOAD="${PIPELINE_IS_INITIAL_LOAD:-false}"

ELASTICSEARCH_HOST="${ELASTICSEARCH_HOST:-10.132.0.33}"
ELASTICSEARCH_BATCH_SIZE="${ELASTICSEARCH_BATCH_SIZE:-10000}"
ELASTICSEARCH_PROPERTIES="${ELASTICSEARCH_PROPERTIES:-}"

java -cp ${PIPELINE_JAR} cc.datafabric.scylladb.pipelines.fulltextload.FullTextLoadPipeline \
    --runner=DataflowRunner \
    --batchSize=${PIPELINE_BATCH_SIZE} --initialLoad=${PIPELINE_IS_INITIAL_LOAD} \
    --elasticsearchHost=${ELASTICSEARCH_HOST} --elasticsearchBatchSize=${ELASTICSEARCH_BATCH_SIZE} \
    --elasticsearchProperties=${ELASTICSEARCH_PROPERTIES} \
    --jobName=scylla-rdf-fulltextload --project=core-datafabric --region=europe-west1 \
    --tempLocation=gs://datafabric-dataflow/temp --gcpTempLocation=gs://datafabric-dataflow/staging \
    --maxNumWorkers=10 --numWorkers=10 --source=gs://fibo-rdf/addresses/*.nt,gs://fibo-rdf/fibo-ru-activities/*,gs://fibo-rdf/fibo-ru/*,gs://fibo-rdf/foreignle/*.nt,gs://fibo-rdf/gov/*.nt,gs://fibo-rdf/individuals/*.nt,gs://fibo-rdf/le/*.nt,gs://fibo-rdf/people/*.nt,gs://fibo-rdf/pif/*.nt,gs://fibo-rdf/rosstat-2012/*.nt,gs://fibo-rdf/rosstat-2013/*.nt,gs://fibo-rdf/rosstat-2014/*.nt,gs://fibo-rdf/rosstat-2015/*.nt,gs://fibo-rdf/rosstat-2016/*.nt,gs://fibo-rdf/rosstat/*,gs://fibo-rdf/ui/*
