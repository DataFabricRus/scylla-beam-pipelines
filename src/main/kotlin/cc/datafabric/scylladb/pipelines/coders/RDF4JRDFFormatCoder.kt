package cc.datafabric.scylladb.pipelines.coders

import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.CoderException
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.coders.StructuredCoder
import org.eclipse.rdf4j.rio.RDFFormat
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.util.Arrays

class RDF4JRDFFormatCoder private constructor() : StructuredCoder<RDFFormat>() {

    private val stringCoder = StringUtf8Coder.of()

    @Throws(CoderException::class, IOException::class)
    override fun encode(value: RDFFormat, outStream: OutputStream) {
        stringCoder.encode(value.name, outStream)
    }

    @Throws(CoderException::class, IOException::class)
    override fun decode(inStream: InputStream): RDFFormat {
        val name = stringCoder.decode(inStream)
        val found = RDF_FORMATS.parallelStream()
            .filter { it.name.equals(name, ignoreCase = true) }
            .findFirst()

        return if (found.isPresent) {
            found.get()
        } else {
            throw IllegalArgumentException()
        }
    }

    override fun getCoderArguments(): List<Coder<*>>? {
        return null
    }

    @Throws(Coder.NonDeterministicException::class)
    override fun verifyDeterministic() {
        stringCoder.verifyDeterministic()
    }

    companion object {

        private val RDF_FORMATS = Arrays.asList(RDFFormat.NTRIPLES, RDFFormat.NQUADS)
        private val INSTANCE = RDF4JRDFFormatCoder()

        fun of(): Coder<RDFFormat> {
            return INSTANCE
        }
    }
}