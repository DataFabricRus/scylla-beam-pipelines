# Apache Beam pipelines for Scylla-RDF

Here you can find pipelines that manipulate RDF data in [Scylla-RDF](https://github.com/DataFabricRus/scylla-rdf).

## Bulk loading

For testing purpose or small files, you can run the pipeline with the embedded Apache Flink. To do so: 
 
  1. set `SCYLLA_RDF_STORAGE_HOSTS` in [docker-compose.yml](https://github.com/DataFabricRus/scylla-beam-pipelines/blob/master/docker-compose.yml)
to hostnames/IPs of the ScyllaDB separated by comma,
  1. put your RDF files in `upload` folder near the docker-compose.yml,
  1. run the pipeline
        ```
        docker-compose up && docker-compose rm -f
        ```

For large files, you can deploy pipeline on [Google Dataflow](https://cloud.google.com/dataflow/) or 
[Apache Flink](https://flink.apache.org/). The other runners could be supported as well.

If you want to use Apache Flink, then:

  1. build the project with `mvn build -DskipTests`,
  1. and upload to the cluster, more about it read in the Deployment & Operations / Clusters & Deployment section in the [Flink's docs](https://ci.apache.org/projects/flink/flink-docs-release-1.7/),
  1. don't forget to use the same parameters as in `run_bulkload_flink.sh`.
  
If you wan to use Google Dataflow, then:

  1. also build the project with `mvn build -DskipTests`,
  1. change parameters in `run_bulkload_dataflow.sh`, so it'd run in your GCP project,
  1. run the script:
      ```
      ./run_bulkload_dataflow.sh --source=gs://<your bucket>/folder1/*,gs://<your bucket>/folder2/*
      ``` 
