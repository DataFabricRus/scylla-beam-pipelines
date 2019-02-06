package cc.datafabric.scylladb.pipelines.bulkload

import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PDone
import org.eclipse.rdf4j.model.Model

class ModelToIndex(private val hosts: List<String>, private val port: Int, private val keyspace: String) {

    fun toSPOC(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input.apply(ParDo.of(SPOCIndexDoFn(hosts, port, keyspace)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    fun toPOSC(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input.apply(ParDo.of(POSCIndexDoFn(hosts, port, keyspace)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    fun toOSPC(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input.apply(ParDo.of(OSPCIndexDoFn(hosts, port, keyspace)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    fun toCSPO(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input.apply(ParDo.of(CSPOIndexDoFn(hosts, port, keyspace)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    fun toCPOS(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input.apply(ParDo.of(CPOSIndexDoFn(hosts, port, keyspace)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    fun toCOSP(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input.apply(ParDo.of(COSPIndexDoFn(hosts, port, keyspace)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    fun toSTAT(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input.apply(ParDo.of(STATIndexDoFn(hosts, port, keyspace)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    class SPOCIndexDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element.forEach {
                val spoc = coder.encode(it)

                add(dao.insertInSPOC(spoc[0], spoc[1], spoc[2], spoc[3]))
            }
        }
    }

    class POSCIndexDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element.forEach {
                val spoc = coder.encode(it)

                add(dao.insertInPOSC(spoc[0], spoc[1], spoc[2], spoc[3]))
            }
        }
    }

    class OSPCIndexDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element.forEach {
                val spoc = coder.encode(it)

                add(dao.insertInOSPC(spoc[0], spoc[1], spoc[2], spoc[3]))
            }
        }
    }

    class CSPOIndexDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element
                .filter { stmt -> stmt.context != null }
                .forEach {
                    val spoc = coder.encode(it)

                    add(dao.insertInCSPO(spoc[0], spoc[1], spoc[2], spoc[3]))
                }
        }
    }

    class CPOSIndexDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element
                .filter { stmt -> stmt.context != null }
                .forEach {
                    val spoc = coder.encode(it)

                    add(dao.insertInCPOS(spoc[0], spoc[1], spoc[2], spoc[3]))
                }
        }
    }

    class COSPIndexDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element
                .filter { stmt -> stmt.context != null }
                .forEach {
                    val spoc = coder.encode(it)

                    add(dao.insertInCOSP(spoc[0], spoc[1], spoc[2], spoc[3]))
                }
        }
    }

    class STATIndexDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            for (stmt in element) {
                val spoc = coder.encode(stmt)

                add(dao.incrementStatistics(spoc[0], spoc[1], spoc[2], spoc[3]))
            }
        }

    }

}