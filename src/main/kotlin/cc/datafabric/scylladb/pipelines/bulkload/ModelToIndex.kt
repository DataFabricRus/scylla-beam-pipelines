package cc.datafabric.scylladb.pipelines.bulkload

import cc.datafabric.scyllardf.dao.ScyllaRDFSchema
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.Sum
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PDone
import org.eclipse.rdf4j.model.Model
import java.nio.ByteBuffer

class ModelToIndex(private val hosts: List<String>, private val port: Int, private val keyspace: String) {

    companion object {

        private const val STAT_C_ID: Byte = 0
        private const val STAT_S_ID: Byte = 1
        private const val STAT_P_ID: Byte = 2
        private const val STAT_O_ID: Byte = 3
        private const val STAT_SP_ID: Byte = 4
        private const val STAT_PO_ID: Byte = 5
        private const val STAT_SO_ID: Byte = 6

    }

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

                input
                    .apply(ParDo.of(STATIndexToKV(hosts, port, keyspace)))
                    .apply(Sum.longsPerKey())
                    .apply(ParDo.of(STATIndexWriterDoFn(hosts, port, keyspace)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    class SPOCIndexDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model, Boolean>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element.forEach {
                val spoc = coder.encode(it)

                dao.insertInSPOC(spoc[0], spoc[1], spoc[2], spoc[3])
            }
        }
    }

    class POSCIndexDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model, Boolean>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element.forEach {
                val spoc = coder.encode(it)

                dao.insertInPOSC(spoc[0], spoc[1], spoc[2], spoc[3])
            }
        }
    }

    class OSPCIndexDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model, Boolean>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element.forEach {
                val spoc = coder.encode(it)

                dao.insertInOSPC(spoc[0], spoc[1], spoc[2], spoc[3])
            }
        }
    }

    class CSPOIndexDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model, Boolean>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element
                .filter { stmt -> stmt.context != null }
                .forEach {
                    val spoc = coder.encode(it)

                    dao.insertInCSPO(spoc[0], spoc[1], spoc[2], spoc[3])
                }
        }
    }

    class CPOSIndexDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model, Boolean>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element
                .filter { stmt -> stmt.context != null }
                .forEach {
                    val spoc = coder.encode(it)

                    dao.insertInCPOS(spoc[0], spoc[1], spoc[2], spoc[3])
                }
        }
    }

    class COSPIndexDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model, Boolean>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element
                .filter { stmt -> stmt.context != null }
                .forEach {
                    val spoc = coder.encode(it)

                    dao.insertInCOSP(spoc[0], spoc[1], spoc[2], spoc[3])
                }
        }
    }

    class STATIndexToKV(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<Model, KV<ByteArray, Long>>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: Model, receiver: OutputReceiver<KV<ByteArray, Long>>) {
            element.forEach { stmt ->
                if(stmt.context == null) {
                    receiver.output(KV.of(concat(STAT_C_ID, ScyllaRDFSchema.CONTEXT_DEFAULT), 1L))
                } else {
                    receiver.output(KV.of(concat(STAT_C_ID, coder.encode(stmt.context)!!), 1L))
                }

                receiver.output(KV.of(concat(STAT_S_ID, coder.encode(stmt.subject)!!), 1L))
                receiver.output(KV.of(concat(STAT_P_ID, coder.encode(stmt.predicate)!!), 1L))
                receiver.output(KV.of(concat(STAT_O_ID, coder.encode(stmt.`object`)!!), 1L))

                receiver.output(KV.of(concat(STAT_SP_ID, coder.encode(stmt.subject)!!, coder.encode(stmt.predicate)!!), 1L))
                receiver.output(KV.of(concat(STAT_PO_ID, coder.encode(stmt.predicate)!!, coder.encode(stmt.`object`)!!), 1L))
                receiver.output(KV.of(concat(STAT_SO_ID, coder.encode(stmt.subject)!!, coder.encode(stmt.`object`)!!), 1L))
            }
        }

        private fun concat(a: Byte, b: ByteBuffer) : ByteArray {
            val arr = ByteArray(1 + b.array().size)

            arr[0] = a;
            System.arraycopy(b.array(), 0, arr, 1, b.array().size)

            return arr
        }

        private fun concat(a: Byte, b: ByteBuffer, c: ByteBuffer) : ByteArray {
            val arr = ByteArray(1 + b.array().size + c.array().size)

            arr[0] = a;
            System.arraycopy(b.array(), 0, arr, 1, b.array().size)
            System.arraycopy(c.array(), 0, arr, 1 + b.array().size, c.array().size)

            return arr
        }

    }

    class STATIndexWriterDoFn(hosts: List<String>, port: Int, keyspace: String)
        : AbstractCassandraExecutor<KV<ByteArray, Long>, Boolean>(hosts, port, keyspace) {

        @ProcessElement
        public fun processElement(@Element element: KV<ByteArray, Long>) {
            val statTypeId = element.key!![0]

            val id = ByteBuffer.allocate(element.key!!.size - 1)
            id.put(element.key!!, 1, element.key!!.size - 1)
            id.rewind()

            when (statTypeId) {
                STAT_C_ID -> batch(dao.incrementStatCBy(id, element.value))
                STAT_S_ID -> batch(dao.incrementStatSBy(id, element.value))
                STAT_P_ID -> batch(dao.incrementStatPBy(id, element.value))
                STAT_O_ID -> batch(dao.incrementStatOBy(id, element.value))
                STAT_SP_ID -> batch(dao.incrementStatSPBy(id, element.value))
                STAT_PO_ID -> batch(dao.incrementStatPOBy(id, element.value))
                STAT_SO_ID -> batch(dao.incrementStatSOBy(id, element.value))
                else -> throw IllegalArgumentException()
            }
        }

    }

}