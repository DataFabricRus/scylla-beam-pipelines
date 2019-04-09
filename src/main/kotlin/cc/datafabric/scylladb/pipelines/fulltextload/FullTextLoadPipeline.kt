package cc.datafabric.scylladb.pipelines.fulltextload

import cc.datafabric.scylladb.pipelines.bulkload.DefaultElasticsearchPipelineOptions
import cc.datafabric.scylladb.pipelines.coders.RDF4JModelCoder
import cc.datafabric.scylladb.pipelines.coders.RDF4JRDFFormatCoder
import cc.datafabric.scylladb.pipelines.io.ElasticsearchInitialLoadIO
import cc.datafabric.scylladb.pipelines.io.ElasticsearchPartialUpdateIO
import cc.datafabric.scylladb.pipelines.io.RDF4JIO
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Create
import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.rio.RDFFormat
import org.slf4j.LoggerFactory
import java.util.Arrays

interface FullTextLoadPipelineOptions : DataflowPipelineOptions, DefaultElasticsearchPipelineOptions {
    var source: String
    var batchSize: Long
    var isInitialLoad: Boolean
}

object FullTextLoadPipeline {

    private val LOG = LoggerFactory.getLogger(FullTextLoadPipeline::class.java)

    private fun create(options: FullTextLoadPipelineOptions): Pipeline {
        val p = Pipeline.create(options)

        p.coderRegistry.registerCoderForClass(RDFFormat::class.java, RDF4JRDFFormatCoder.of())
        p.coderRegistry.registerCoderForClass(Model::class.java, RDF4JModelCoder.of())

        val models = p
            .apply(Create.of(options.source))
            .apply("Read triples", RDF4JIO.Read(options.batchSize))

        if (options.isInitialLoad) {
            models.apply("Write Elasticsearch Index", ElasticsearchInitialLoadIO(
                options.elasticsearchHost, options.elasticsearchBatchSize, options.elasticsearchProperties
            ))
        } else {
            models.apply("Write Elasticsearch Index", ElasticsearchPartialUpdateIO(
                options.elasticsearchHost, options.elasticsearchBatchSize, options.elasticsearchProperties
            ))
        }

        return p
    }

    @JvmStatic
    public fun main(args: Array<String>) {
        val options = PipelineOptionsFactory
            .fromArgs(*args)
            .withValidation()
            .`as`(FullTextLoadPipelineOptions::class.java)

        if(options.elasticsearchProperties.isNullOrEmpty() || options.elasticsearchProperties!![0].isBlank()) {
            LOG.info("Will accept all literals for indexing")

            options.elasticsearchProperties = null
        } else {
            LOG.info("Only the following props will be accepted for indexing: {}", Arrays.toString(options.elasticsearchProperties))
        }

        FullTextLoadPipeline.create(options).run()
    }

}