package cc.datafabric.scylladb.pipelines.bulkload

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions

interface DefaultCassandraPipelineOptions : DataflowPipelineOptions {
    var hosts: String
    var port: Int
    var keyspace: String
}