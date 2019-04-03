FROM openjdk:8-jre

ENV PIPELINE_JAR=./scylla-rdf-pipelines.jar

COPY target/scylladb-pipelines-0.0.1-SNAPSHOT.jar $PIPELINE_JAR
COPY run_bulkload_flink.sh run_bulkload_dataflow.sh ./

CMD ./run_bulkload_flink.sh