package cc.datafabric.scylladb.pipelines.bulkload

import cc.datafabric.scylladb.pipelines.coders.RDF4JModelCoder
import cc.datafabric.scylladb.pipelines.coders.RDFFormatCoder
import cc.datafabric.scylladb.pipelines.io.RDF4JIO
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.PipelineRunner
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.io.cassandra.CassandraIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.rio.RDFFormat

interface BulkLoadPipelineOptions : DefaultCassandraPipelineOptions {
    var source: String
    var batchSize: Long
}

object BulkLoadPipeline {

    public fun create(options: BulkLoadPipelineOptions): Pipeline {
        val p = Pipeline.create(options)

        p.coderRegistry.registerCoderForClass(RDFFormat::class.java, RDFFormatCoder.of())
        p.coderRegistry.registerCoderForClass(Model::class.java, RDF4JModelCoder.of())
        p.coderRegistry.registerCoderForClass(SPOTriple::class.java, SerializableCoder.of(SPOTriple::class.java))

        p
            .apply(Create.of(options.source))
            .apply("Read triples", RDF4JIO.Read(options.batchSize))
            .apply(ModelToSPOTriple())
            .apply("Write rows", CassandraIO
                .write<SPOTriple>()
                .withHosts(options.hosts.split(","))
                .withPort(options.port)
                .withKeyspace(options.keyspace)
                .withConsistencyLevel("ANY")
                .withEntity(SPOTriple::class.java)
            )

        return p
    }

    private class ModelToSPOTriple : PTransform<PCollection<Model>, PCollection<SPOTriple>>() {

        override fun expand(input: PCollection<Model>): PCollection<SPOTriple> {
            return input.apply(ParDo.of(object : DoFn<Model, SPOTriple>() {
                @ProcessElement
                public fun processElement(@Element element: Model, receiver: OutputReceiver<SPOTriple>) {
                    element.forEach { receiver.output(SPOTriple(it)) }
                }
            }))
        }

    }

    @JvmStatic
    public fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.`as`(BulkLoadPipelineOptions::class.java)

        options.jobName = "scylladb-bulkload"
        options.project = "core-datafabric"
        options.region = "europe-west1"
        options.tempLocation = "gs://datafabric-dataflow/temp"
        options.gcpTempLocation = "gs://datafabric-dataflow/staging"
        options.runner = Class.forName("org.apache.beam.runners.dataflow.DataflowRunner") as Class<PipelineRunner<*>>
//        options.setRunner((Class<PipelineRunner<?>>) Class.forName("org.apache.beam.runners.direct.DirectRunner"));
        options.maxNumWorkers = 30
        options.numWorkers = 30

        options.hosts = "10.132.0.29,10.132.0.40"
        options.port = 9042
        options.keyspace = "triplestore"

//        options.source = "gs://fibo-rdf/addresses/*.nt,gs://fibo-rdf/fibo-ru-activities/*,gs://fibo-rdf/fibo-ru/*," +
//                "gs://fibo-rdf/foreignle/*.nt,gs://fibo-rdf/gov/*.nt,gs://fibo-rdf/individuals/*.nt," +
//                "gs://fibo-rdf/le/*.nt,gs://fibo-rdf/people/*.nt,gs://fibo-rdf/pif/*.nt,gs://fibo-rdf/rosstat-2012/*.nt," +
//                "gs://fibo-rdf/rosstat-2013/*.nt,gs://fibo-rdf/rosstat-2014/*.nt,gs://fibo-rdf/rosstat-2015/*.nt," +
//                "gs://fibo-rdf/rosstat-2016/*.nt,gs://fibo-rdf/rosstat/*,gs://fibo-rdf/ui/*"
        options.source =
            "gs://fibo-rdf/rosstat-2013/*.nt"
        options.batchSize = 500000

        BulkLoadPipeline.create(options).run()
    }

}