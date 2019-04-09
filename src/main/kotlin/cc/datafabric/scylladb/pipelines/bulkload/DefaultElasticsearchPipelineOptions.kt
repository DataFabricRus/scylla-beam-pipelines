package cc.datafabric.scylladb.pipelines.bulkload

import org.apache.beam.sdk.options.PipelineOptions

interface DefaultElasticsearchPipelineOptions : PipelineOptions {
    var elasticsearchHost: String
    var elasticsearchBatchSize: Long
    var elasticsearchProperties: Array<String>?
}