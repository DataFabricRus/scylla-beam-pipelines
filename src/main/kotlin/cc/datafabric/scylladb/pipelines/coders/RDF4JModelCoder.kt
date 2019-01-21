package cc.datafabric.scylladb.pipelines.coders

import com.google.common.io.ByteStreams
import com.google.common.io.CountingOutputStream
import org.apache.avro.util.ByteBufferOutputStream
import org.apache.beam.sdk.coders.AtomicCoder
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.CoderException
import org.apache.commons.io.input.CloseShieldInputStream
import org.apache.beam.sdk.util.VarInt
import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.rio.Rio
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

class RDF4JModelCoder private constructor() : AtomicCoder<Model>() {

    @Throws(IOException::class)
    override fun encode(value: Model, outStream: OutputStream) {
        val bos = ByteBufferOutputStream()

        Rio.write(value, bos, RDF_FORMAT)

        val bufList = bos.bufferList
        val length = bufList.stream().reduce(0L, { l, bb -> l!! + bb.limit() }, { l1, l2 -> l1!! + l2!! })
        VarInt.encode(length, outStream)

        for (buf in bufList) {
            outStream.write(buf.array(), 0, buf.limit())
        }

    }

    @Throws(CoderException::class, IOException::class)
    override fun decode(inStream: InputStream): Model {
        /*
         *  Read the size of the value in the stream. To make sure that we don't read any byte of another value.
         */
        val length = VarInt.decodeLong(inStream)
        if (length < 0) {
            throw IOException("Invalid length $length")
        }

        val limited = CloseShieldInputStream(ByteStreams.limit(inStream, length))

        return Rio.parse(limited, "", RDF_FORMAT)
    }

    override fun isRegisterByteSizeObserverCheap(value: Model?): Boolean {
        return false
    }

    @Throws(Exception::class)
    override fun getEncodedElementByteSize(value: Model): Long {
        CountingOutputStream(ByteStreams.nullOutputStream()).use { cos ->
            Rio.write(value, cos, RDF_FORMAT)

            val count = cos.count

            return VarInt.getLength(count) + count
        }
    }

    companion object {

        private val RDF_FORMAT = RDFFormat.BINARY
        private val INSTANCE = RDF4JModelCoder()

        fun of(): Coder<Model> {
            return INSTANCE
        }
    }
}
