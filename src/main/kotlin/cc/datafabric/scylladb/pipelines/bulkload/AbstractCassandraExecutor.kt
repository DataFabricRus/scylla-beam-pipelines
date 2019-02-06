package cc.datafabric.scylladb.pipelines.bulkload

import cc.datafabric.scyllardf.coder.CoderFacade
import cc.datafabric.scyllardf.dao.ScyllaRDFDAO
import com.datastax.driver.core.ResultSetFuture
import com.google.common.util.concurrent.Futures
import org.apache.beam.sdk.transforms.DoFn
import java.net.InetAddress

open class AbstractCassandraExecutor<T>(
    private val hosts: List<String>, private val port: Int, protected val keyspace: String
) : DoFn<T, Boolean>() {

    companion object {
        private const val CONCURRENT_ASYNC_QUERIES = 100
    }

    protected lateinit var dao: ScyllaRDFDAO
    protected lateinit var coder: CoderFacade

    private var batch = newBatch()

    @Setup
    public open fun setup() {
        dao = ScyllaRDFDAO.create(hosts.map { InetAddress.getByName(it) }, port, keyspace)

        coder = CoderFacade
        coder.initialize(dao)
    }

    @FinishBundle
    public fun finishBundle() {
        checkBatchSize()
    }

    @Teardown
    public fun tearDown() {
        dao.close()
    }

    protected fun add(future: ResultSetFuture) {
        checkBatchSize()

        batch.add(future)
    }

    protected fun add(futures: List<ResultSetFuture>) {
        checkBatchSize()

        batch.addAll(futures)
    }

    private fun checkBatchSize() {
        if (batch.size > CONCURRENT_ASYNC_QUERIES) {
            Futures.allAsList(batch).get()

            batch = newBatch()
        }
    }

    private fun newBatch(): MutableList<ResultSetFuture> {
        return ArrayList(CONCURRENT_ASYNC_QUERIES)
    }

}