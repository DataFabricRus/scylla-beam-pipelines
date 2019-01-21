package cc.datafabric.scylladb.pipelines.bulkload

import com.datastax.driver.mapping.annotations.ClusteringColumn
import com.datastax.driver.mapping.annotations.Column
import com.datastax.driver.mapping.annotations.PartitionKey
import com.datastax.driver.mapping.annotations.Table
import org.eclipse.rdf4j.model.Statement
import java.io.Serializable
import java.util.Objects

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

    override fun equals(other: Any?): Boolean {
        if(other == null || !(other is SPOTriple)) {
            return false
        }

        return Objects.equals(this.subject, other.subject) &&
            Objects.equals(this.predicate, other.predicate) &&
            Objects.equals(this.`object`, other.`object`)
    }

    override fun hashCode(): Int {
        var result = subject.hashCode()
        result = 31 * result + predicate.hashCode()
        result = 31 * result + `object`.hashCode()
        return result
    }
}