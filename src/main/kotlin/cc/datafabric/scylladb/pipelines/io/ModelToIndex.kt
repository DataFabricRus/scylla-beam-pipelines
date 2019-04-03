package cc.datafabric.scylladb.pipelines.io

import cc.datafabric.scyllardf.dao.ScyllaRDFSchema
import org.apache.beam.sdk.coders.ByteArrayCoder
import org.apache.beam.sdk.coders.ByteCoder
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.ListCoder
import org.apache.beam.sdk.coders.StructuredCoder
import org.apache.beam.sdk.coders.VarLongCoder
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.Sum
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PDone
import org.eclipse.rdf4j.model.Model
import java.io.InputStream
import java.io.OutputStream
import java.io.Serializable
import java.nio.ByteBuffer
import java.util.Arrays
import java.util.Collections
import java.util.Objects

class ModelToIndex(
    private val hosts: List<String>,
    private val port: Int,
    private val keyspace: String,
    private val maxRequestsPerConnection: Int
) {

    companion object {

        private const val CARD_C_ID: Byte = 0
        private const val CARD_P_ID: Byte = 1
        private const val CARD_PO_ID: Byte = 2

    }

    fun initialize() {

    }

    fun toSPOC(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input.apply(ParDo.of(SPOCIndexDoFn(hosts, port, keyspace, maxRequestsPerConnection)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    fun toPOSC(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input.apply(ParDo.of(POSCIndexDoFn(hosts, port, keyspace, maxRequestsPerConnection)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    fun toOSPC(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input.apply(ParDo.of(OSPCIndexDoFn(hosts, port, keyspace, maxRequestsPerConnection)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    fun toCSPO(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input.apply(ParDo.of(CSPOIndexDoFn(hosts, port, keyspace, maxRequestsPerConnection)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    fun toCPOS(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input.apply(ParDo.of(CPOSIndexDoFn(hosts, port, keyspace, maxRequestsPerConnection)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    fun toCOSP(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input.apply(ParDo.of(COSPIndexDoFn(hosts, port, keyspace, maxRequestsPerConnection)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    fun toCARD(): PTransform<PCollection<Model>, PDone> {
        return object : PTransform<PCollection<Model>, PDone>() {
            override fun expand(input: PCollection<Model>): PDone {

                input
                    .apply(ParDo.of(STATIndexToKV(hosts, port, keyspace, maxRequestsPerConnection)))
                    .setCoder(KvCoder.of(CardKeyCoder(), VarLongCoder.of()))
                    .apply(Sum.longsPerKey())
                    .apply(ParDo.of(STATIndexWriterDoFn(hosts, port, keyspace, maxRequestsPerConnection)))

                return PDone.`in`(input.pipeline)
            }
        }
    }

    class SPOCIndexDoFn(hosts: List<String>, port: Int, keyspace: String, maxRequestsPerConnection: Int)
        : AbstractCassandraExecutor<Model, Boolean>(hosts, port, keyspace, maxRequestsPerConnection) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element.forEach {
                val spoc = coder.encode(it)

                batch(indexDAO.insertInSPOC(spoc[0], spoc[1], spoc[2], spoc[3]))
            }
        }
    }

    class POSCIndexDoFn(hosts: List<String>, port: Int, keyspace: String, maxRequestsPerConnection: Int)
        : AbstractCassandraExecutor<Model, Boolean>(hosts, port, keyspace, maxRequestsPerConnection) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element.forEach {
                val spoc = coder.encode(it)

                batch(indexDAO.insertInPOSC(spoc[0], spoc[1], spoc[2], spoc[3]))
            }
        }
    }

    class OSPCIndexDoFn(hosts: List<String>, port: Int, keyspace: String, maxRequestsPerConnection: Int)
        : AbstractCassandraExecutor<Model, Boolean>(hosts, port, keyspace, maxRequestsPerConnection) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element.forEach {
                val spoc = coder.encode(it)

                batch(indexDAO.insertInOSPC(spoc[0], spoc[1], spoc[2], spoc[3]))
            }
        }
    }

    class CSPOIndexDoFn(hosts: List<String>, port: Int, keyspace: String, maxRequestsPerConnection: Int)
        : AbstractCassandraExecutor<Model, Boolean>(hosts, port, keyspace, maxRequestsPerConnection) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element
                .filter { stmt -> stmt.context != null }
                .forEach {
                    val spoc = coder.encode(it)

                    batch(indexDAO.insertInCSPO(spoc[0], spoc[1], spoc[2], spoc[3]))
                }
        }
    }

    class CPOSIndexDoFn(hosts: List<String>, port: Int, keyspace: String, maxRequestsPerConnection: Int)
        : AbstractCassandraExecutor<Model, Boolean>(hosts, port, keyspace, maxRequestsPerConnection) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element
                .filter { stmt -> stmt.context != null }
                .forEach {
                    val spoc = coder.encode(it)

                    batch(indexDAO.insertInCPOS(spoc[0], spoc[1], spoc[2], spoc[3]))
                }
        }
    }

    class COSPIndexDoFn(hosts: List<String>, port: Int, keyspace: String, maxRequestsPerConnection: Int)
        : AbstractCassandraExecutor<Model, Boolean>(hosts, port, keyspace, maxRequestsPerConnection) {

        @ProcessElement
        public fun processElement(@Element element: Model) {
            element
                .filter { stmt -> stmt.context != null }
                .forEach {
                    val spoc = coder.encode(it)

                    batch(indexDAO.insertInCOSP(spoc[0], spoc[1], spoc[2], spoc[3]))
                }
        }
    }

    class STATIndexToKV(hosts: List<String>, port: Int, keyspace: String, maxRequestsPerConnection: Int)
        : AbstractCassandraExecutor<Model, KV<CardKey, Long>>(hosts, port, keyspace, maxRequestsPerConnection) {

        @ProcessElement
        public fun processElement(@Element element: Model, receiver: OutputReceiver<KV<CardKey, Long>>) {
            element.forEach { stmt ->
                if (stmt.context == null) {
                    receiver.output(KV.of(CardKey(CARD_C_ID, ScyllaRDFSchema.CONTEXT_DEFAULT), 1L))
                } else {
                    receiver.output(KV.of(CardKey(CARD_C_ID, coder.encode(stmt.context)!!), 1L))
                }

                receiver.output(KV.of(CardKey(CARD_P_ID, coder.encode(stmt.predicate)!!), 1L))
                receiver.output(KV.of(CardKey(CARD_PO_ID, coder.encode(stmt.predicate)!!, coder.encode(stmt.`object`)!!), 1L))
            }
        }

    }

    class STATIndexWriterDoFn(hosts: List<String>, port: Int, keyspace: String, maxRequestsPerConnection: Int)
        : AbstractCassandraExecutor<KV<CardKey, Long>, Boolean>(hosts, port, keyspace, maxRequestsPerConnection) {

        @ProcessElement
        public fun processElement(@Element element: KV<CardKey, Long>) {
            val key = element.key!!

            when (key.type) {
                CARD_C_ID -> batch(cardinalityDao.incrementCardC(key.ids[0], element.value))
                CARD_P_ID -> batch(cardinalityDao.incrementCardP(key.ids[0], element.value))
                CARD_PO_ID -> batch(cardinalityDao.incrementCardPO(key.ids[0], key.ids[1], element.value))
                else -> throw IllegalArgumentException()
            }
        }

    }

    class CardKey : Serializable {

        val type: Byte
        val ids: Array<ByteBuffer>

        constructor(type: Byte, id1: ByteBuffer) {
            this.type = type
            this.ids = arrayOf(id1)
        }

        constructor(idType: Byte, id1: ByteBuffer, id2: ByteBuffer) {
            this.type = idType
            this.ids = arrayOf(id1, id2)
        }

        constructor(type: Byte, ids: Array<ByteBuffer>) {
            this.type = type
            this.ids = ids
        }

        override fun equals(other: Any?): Boolean {
            if (other is CardKey) {
                return this.type == other.type && Arrays.deepEquals(this.ids, other.ids)
            }

            return false
        }

        override fun hashCode(): Int {
            return type.hashCode() + Objects.hash(*ids)
        }

    }

    class CardKeyCoder : StructuredCoder<CardKey>() {

        private val byteCoder = ByteCoder.of()
        private val byteArrayCoder = ByteArrayCoder.of()
        private val arrayCoder = ListCoder.of(byteArrayCoder)

        override fun getCoderArguments(): MutableList<out Coder<*>> {
            return Collections.emptyList()
        }

        override fun verifyDeterministic() {
            byteCoder.verifyDeterministic()
            byteArrayCoder.verifyDeterministic()
            arrayCoder.verifyDeterministic()
        }

        override fun encode(value: CardKey, outStream: OutputStream?) {
            byteCoder.encode(value.type, outStream)

            val arr = value.ids.map { it.array() }.toMutableList()
            arrayCoder.encode(arr, outStream)
        }

        override fun decode(inStream: InputStream): CardKey {
            val type = byteCoder.decode(inStream)
            val ids = arrayCoder.decode(inStream).map { ByteBuffer.wrap(it) }.toTypedArray()
            return CardKey(type, ids)
        }
    }
}