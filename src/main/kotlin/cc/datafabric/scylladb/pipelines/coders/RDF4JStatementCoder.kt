package cc.datafabric.scylladb.pipelines.coders

import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.NullableCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.coders.StructuredCoder
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.model.impl.SimpleValueFactory
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil
import java.io.InputStream
import java.io.OutputStream

class RDF4JStatementCoder private constructor() : StructuredCoder<Statement>() {

    companion object {

        private val INSTANCE = RDF4JStatementCoder()

        fun of(): Coder<Statement> {
            return INSTANCE
        }
    }

    private val stringCoder = NullableCoder.of(StringUtf8Coder.of())

    override fun getCoderArguments(): MutableList<out Coder<*>> {
        return mutableListOf()
    }

    override fun verifyDeterministic() {
        stringCoder.verifyDeterministic()
    }

    override fun encode(value: Statement, outStream: OutputStream) {
        stringCoder.encode(NTriplesUtil.toNTriplesString(value.subject), outStream)
        stringCoder.encode(value.predicate.stringValue(), outStream)
        stringCoder.encode(NTriplesUtil.toNTriplesString(value.`object`), outStream)
        stringCoder.encode(value.context?.stringValue(), outStream)
    }

    override fun decode(inStream: InputStream): Statement {
        val valueFactory = SimpleValueFactory.getInstance()

        val subj = NTriplesUtil.parseResource(stringCoder.decode(inStream), valueFactory)
        val pred = valueFactory.createIRI(stringCoder.decode(inStream))
        val obj = NTriplesUtil.parseValue(stringCoder.decode(inStream), valueFactory)

        val c = stringCoder.decode(inStream)
        return if (c == null) {
            valueFactory.createStatement(subj, pred, obj)
        } else {
            valueFactory.createStatement(subj, pred, obj, NTriplesUtil.parseResource(c, valueFactory))
        }
    }
}