FROM openjdk:8-jre

ENV SCYLLA_RDF_PIPELINES_JAR=./scylla-rdf-pipelines.jar

COPY target/scylladb-pipelines-0.0.1-SNAPSHOT.jar $SCYLLA_RDF_PIPELINES_JAR
COPY run_bulkload_flink.sh run_bulkload_dataflow.sh ./

CMD ./run_bulkload_flink.sh