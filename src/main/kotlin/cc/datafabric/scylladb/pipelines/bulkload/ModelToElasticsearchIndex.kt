package cc.datafabric.scylladb.pipelines.bulkload

import cc.datafabric.scylladb.pipelines.coders.RDF4JStatementCoder
import cc.datafabric.scylladb.pipelines.transforms.GroupIntoLocalBatches
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.GroupByKey
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PDone
import org.eclipse.rdf4j.model.Literal
import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.sail.elasticsearch.ElasticsearchDocument
import org.eclipse.rdf4j.sail.elasticsearch.ElasticsearchIndex
import org.eclipse.rdf4j.sail.lucene.BulkUpdater
import org.eclipse.rdf4j.sail.lucene.SearchFields
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.Objects
import java.util.Properties

class ModelToElasticsearchIndex(
    private val elasticsearchHost: String,
    private val batchSize: Long
) : PTransform<PCollection<Model>, PDone>() {

    companion object {
        private val LOG = LoggerFactory.getLogger(ModelToElasticsearchIndex::class.java)
    }

    override fun expand(input: PCollection<Model>): PDone {
        input
            .apply(ParDo.of(ModelToSubjectStatements()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), RDF4JStatementCoder.of()))
            .apply(GroupByKey.create())
            .apply(GroupIntoLocalBatches.of(batchSize))
            .apply(ParDo.of(WriteToElasticsearchIndex()))

        return PDone.`in`(input.pipeline)
    }

    private class ModelToSubjectStatements : DoFn<Model, KV<String, Statement>>() {

        @ProcessElement
        fun processElement(@Element model: Model, receiver: OutputReceiver<KV<String, Statement>>) {
            model.forEach {
                if(it.`object` is Literal) {
                    receiver.output(KV.of(it.subject.stringValue(), it))
                }
            }
        }

    }

    private inner class WriteToElasticsearchIndex
        : DoFn<Iterable<@JvmSuppressWildcards KV<String, Iterable<@JvmSuppressWildcards Statement>>>, Boolean>() {

        private lateinit var index: OpenElasticsearchIndex

        @Setup
        @Throws(Exception::class)
        fun setup() {
            /**
             * @see https://github.com/elastic/elasticsearch/issues/25741
             */
            System.setProperty("es.set.netty.runtime.available.processors", "false")

            val properties = Properties()
            properties.setProperty(ElasticsearchIndex.TRANSPORT_KEY, elasticsearchHost)

            index = OpenElasticsearchIndex()
            index.initialize(properties)

            LOG.info("Connected to Elasticsearch on {}", elasticsearchHost)
        }

        @Teardown
        @Throws(IOException::class)
        fun tearDown() {
            index.shutDown()
        }

        @ProcessElement
        @Throws(IOException::class)
        fun processElement(
            @Element documents: Iterable<@JvmSuppressWildcards KV<String, Iterable<Statement>>>, receiver: DoFn.OutputReceiver<Boolean>
        ) {
            val start = System.currentTimeMillis()

            val bulkUpdater = index.newBulkUpdate()

            for (entry in documents) {
                val doc = ElasticsearchDocument(
                    SearchFields.formIdString(Objects.requireNonNull(entry.key),
                        null),
                    ElasticsearchIndex.DEFAULT_DOCUMENT_TYPE,
                    ElasticsearchIndex.DEFAULT_INDEX_NAME,
                    entry.key,
                    SearchFields.getContextID(null), null
                )

                entry.value.forEach {
                    doc.addProperty(
                        SearchFields.getPropertyField(it.predicate),
                        SearchFields.getLiteralPropertyValueAsString(it)
                    )
                }

                bulkUpdater.add(doc)
            }

            bulkUpdater.end()

            receiver.output(true)

            LOG.info("Wrote a batch in {} ms", System.currentTimeMillis() - start)
        }

    }

    private class OpenElasticsearchIndex : ElasticsearchIndex() {

        public override fun newBulkUpdate(): BulkUpdater {
            return super.newBulkUpdate()
        }

    }
}