package cc.datafabric.scylladb.pipelines.bulkload

import cc.datafabric.scylladb.pipelines.coders.RDF4JModelCoder
import cc.datafabric.scylladb.pipelines.coders.RDFFormatCoder
import cc.datafabric.scylladb.pipelines.io.RDF4JIO
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.PipelineRunner
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Create
import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.rio.RDFFormat

interface BulkLoadPipelineOptions : DefaultCassandraPipelineOptions {
    var source: String
    var batchSize: Long
}

object BulkLoadPipeline {

    private const val HOSTS_SEPARATOR = ","

    fun create(options: BulkLoadPipelineOptions): Pipeline {
        val p = Pipeline.create(options)

        p.coderRegistry.registerCoderForClass(RDFFormat::class.java, RDFFormatCoder.of())
        p.coderRegistry.registerCoderForClass(Model::class.java, RDF4JModelCoder.of())

        val modelToIndex = ModelToIndex(options.hosts.split(HOSTS_SEPARATOR), options.port, options.keyspace)

        val models = p
            .apply(Create.of(options.source))
            .apply("Read triples", RDF4JIO.Read(options.batchSize))

        models.apply("Write SPOC Index", modelToIndex.toSPOC())

        models.apply("Write POSC Index", modelToIndex.toPOSC())

        models.apply("Write OSPC Index", modelToIndex.toOSPC())

        models.apply("Write CSPO Index", modelToIndex.toCSPO())

        models.apply("Write CPOS Index", modelToIndex.toCPOS())

        models.apply("Write COSP Index", modelToIndex.toCOSP())

        models.apply("Write STAT Indexes", modelToIndex.toSTAT())

        return p
    }

    @JvmStatic
    public fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.`as`(BulkLoadPipelineOptions::class.java)

        options.jobName = "scylladb-bulkload"
        options.project = "core-datafabric"
        options.region = "europe-west1"
//        options.tempLocation = "gs://datafabric-dataflow/temp"
//        options.gcpTempLocation = "gs://datafabric-dataflow/staging"
//        options.runner = Class.forName("org.apache.beam.runners.dataflow.DataflowRunner") as Class<PipelineRunner<*>>
        options.runner = Class.forName("org.apache.beam.runners.direct.DirectRunner") as Class<PipelineRunner<*>>
        options.maxNumWorkers = 1
        options.numWorkers = 1

        options.hosts = "localhost"
        options.port = 9042
        options.keyspace = "triplestore"

//        options.source = "gs://fibo-rdf/addresses/*.nt,gs://fibo-rdf/fibo-ru-activities/*,gs://fibo-rdf/fibo-ru/*," +
//                "gs://fibo-rdf/foreignle/*.nt,gs://fibo-rdf/gov/*.nt,gs://fibo-rdf/individuals/*.nt," +
//                "gs://fibo-rdf/le/*.nt,gs://fibo-rdf/people/*.nt,gs://fibo-rdf/pif/*.nt,gs://fibo-rdf/rosstat-2012/*.nt," +
//                "gs://fibo-rdf/rosstat-2013/*.nt,gs://fibo-rdf/rosstat-2014/*.nt,gs://fibo-rdf/rosstat-2015/*.nt," +
//                "gs://fibo-rdf/rosstat-2016/*.nt,gs://fibo-rdf/rosstat/*,gs://fibo-rdf/ui/*"
        options.source = "gs://fibo-rdf/le/le-01992-of-03159.nt"
        options.batchSize = 500000

        BulkLoadPipeline.create(options).run()
    }

}