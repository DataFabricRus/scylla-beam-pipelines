package cc.datafabric.scylladb.pipelines.transforms

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.eclipse.rdf4j.model.Literal
import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.model.Statement

class FilterStatements {

    companion object {

        fun filterAndGroupBySubject(properties: Array<String>?): ParDo.SingleOutput<Model, KV<String, Statement>>? {
            return ParDo.of(object : DoFn<Model, KV<String, Statement>>() {

                @ProcessElement
                fun processElement(@Element model: Model, receiver: OutputReceiver<KV<String, Statement>>) {
                    model.forEach {
                        if (accept(properties, it)) {
                            receiver.output(KV.of(it.subject.stringValue(), it))
                        }
                    }
                }
            })
        }

        fun filter(properties: Array<String>?): ParDo.SingleOutput<Model, Statement>? {
            return ParDo.of(object : DoFn<Model, Statement>() {

                @ProcessElement
                fun processElement(@Element model: Model, receiver: OutputReceiver<Statement>) {
                    model.forEach {
                        if (accept(properties, it)) {
                            receiver.output(it)
                        }
                    }
                }
            })
        }

        private fun accept(properties: Array<String>?, stmt: Statement): Boolean {
            if (stmt.`object` is Literal) {
                if (properties == null || properties.isEmpty() || stmt.predicate.stringValue() in properties) {
                    return true
                }
            }

            return false
        }
    }

}