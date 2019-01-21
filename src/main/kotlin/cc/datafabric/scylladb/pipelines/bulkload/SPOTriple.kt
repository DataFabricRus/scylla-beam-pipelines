package cc.datafabric.scylladb.pipelines.bulkload

import com.datastax.driver.mapping.annotations.ClusteringColumn
import com.datastax.driver.mapping.annotations.Column
import com.datastax.driver.mapping.annotations.PartitionKey
import com.datastax.driver.mapping.annotations.Table
import org.eclipse.rdf4j.model.Statement
import java.io.Serializable

@Table(name = "spo", keyspace = "triplestore")
class SPOTriple() : Serializable {

    @PartitionKey(0)
    @Column(name = "subject")
    lateinit var subject: String

    @ClusteringColumn(0)
    @Column(name = "predicate")
    lateinit var predicate: String

    @ClusteringColumn(1)
    @Column(name = "object")
    lateinit var `object`: String

    constructor(stmt: Statement) : this() {
        this.subject = stmt.subject.stringValue()
        this.predicate = stmt.predicate.stringValue()
        this.`object` = stmt.`object`.stringValue()
    }
}