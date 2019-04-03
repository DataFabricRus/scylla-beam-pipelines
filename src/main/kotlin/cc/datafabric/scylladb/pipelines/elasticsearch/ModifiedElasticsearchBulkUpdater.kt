/*******************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package cc.datafabric.scylladb.pipelines.elasticsearch

import org.eclipse.rdf4j.sail.elasticsearch.ElasticsearchDocument
import org.eclipse.rdf4j.sail.lucene.BulkUpdater
import org.eclipse.rdf4j.sail.lucene.SearchDocument
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptType
import java.io.IOException

class ModifiedElasticsearchBulkUpdater(private val client: Client) : BulkUpdater {

    companion object {
        private const val PARTIAL_UPDATE_SCRIPT = """
            if(ctx._source.text instanceof List) {
                if(!ctx._source.text.contains(params.text)) {
                    ctx._source.text.add(params.text)
                }
            } else if(ctx._source.text != params.text) {
                ctx._source.text = [ctx._source.text, params.text]
            }
            if(ctx._source.containsKey(params.property)) {
                if(ctx._source[params.property] instanceof List) {
                    if(!ctx._source[params.property].contains(params.text)) {
                        ctx._source[params.property].add(params.text)
                    }
                } else if(ctx._source[params.property] != params.text) {
                    ctx._source[params.property] = [ctx._source[params.property], params.text]
                }
            } else {
                ctx._source[params.property] = params.text
            }
        """
    }

    private val bulkRequest: BulkRequestBuilder = client.prepareBulk()

    @Throws(IOException::class)
    override fun add(doc: SearchDocument) {
        val esDoc = doc as ElasticsearchDocument
        bulkRequest.add(
            client.prepareIndex(esDoc.index, esDoc.type, esDoc.id).setSource(esDoc.source))
    }

    @Throws(IOException::class)
    override fun update(doc: SearchDocument) {
        val esDoc = doc as ElasticsearchDocument
        bulkRequest.add(client.prepareUpdate(esDoc.index, esDoc.type, esDoc.id)
            .setVersion(esDoc.version)
            .setDoc(esDoc.source))
    }

    fun scriptedPartialUpdate(doc: SearchDocument, propertyName: String) {
        val esDoc = doc as ElasticsearchDocument
        val fieldName = ModifiedElasticsearchIndex.toPropertyFieldName(propertyName)

        val fieldValue = esDoc.source[fieldName]
        if (fieldValue != null) {
            val update = client.prepareUpdate(esDoc.index, esDoc.type, esDoc.id)
                .setVersion(esDoc.version)
                .setScript(Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, PARTIAL_UPDATE_SCRIPT,
                    mapOf(
                        Pair("text", fieldValue),
                        Pair("property", fieldName)
                    )
                ))
                .setScriptedUpsert(false)
                .setUpsert(esDoc.source)

            bulkRequest.add(update)
        }
    }

    @Throws(IOException::class)
    override fun delete(doc: SearchDocument) {
        val esDoc = doc as ElasticsearchDocument
        bulkRequest.add(
            client.prepareDelete(esDoc.index, esDoc.type, esDoc.id).setVersion(esDoc.version))
    }

    @Throws(IOException::class)
    override fun end() {
        if (bulkRequest.numberOfActions() > 0) {
            val response = bulkRequest.execute().actionGet()
            if (response.hasFailures()) {
                System.out.println(response.items[0]?.failure?.toString())
                throw IOException(response.buildFailureMessage())
            }
        }
    }
}
