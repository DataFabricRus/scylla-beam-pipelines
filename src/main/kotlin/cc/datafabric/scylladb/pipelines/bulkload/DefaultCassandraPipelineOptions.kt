package cc.datafabric.scylladb.pipelines.bulkload

import org.apache.beam.sdk.options.PipelineOptions

interface DefaultCassandraPipelineOptions : PipelineOptions {
    var hosts: String
    var port: Int
    var keyspace: String
    var maxRequestsPerConnection: Int
}