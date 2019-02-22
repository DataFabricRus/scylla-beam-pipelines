package cc.datafabric.scylladb.pipelines.bulkload

import cc.datafabric.scyllardf.coder.CoderFacade
import cc.datafabric.scyllardf.dao.ScyllaRDFDAO
import com.datastax.driver.core.HostDistance
import com.datastax.driver.core.PoolingOptions
import com.datastax.driver.core.ResultSetFuture
import com.google.common.util.concurrent.Futures
import org.apache.beam.sdk.transforms.DoFn
import org.slf4j.LoggerFactory
import java.net.InetAddress

open class AbstractCassandraExecutor<InputT, OutputT>(
    private val hosts: List<String>, private val port: Int, protected val keyspace: String
) : DoFn<InputT, OutputT>() {

    companion object {
        private const val MAX_BATCH_SIZE = 512

        private val LOG = LoggerFactory.getLogger(AbstractCassandraExecutor::class.java)
    }

    protected lateinit var dao: ScyllaRDFDAO
    protected lateinit var coder: CoderFacade

    private var batch = newBatch()

    @Setup
    public open fun setup() {
        dao = ScyllaRDFDAO.create(hosts.map { InetAddress.getByName(it) }, port, keyspace, PoolingOptions()
            .setMaxRequestsPerConnection(HostDistance.LOCAL, MAX_BATCH_SIZE)
        )

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

    protected fun batch(future: ResultSetFuture) {
        checkBatchSize()

        batch.add(future)
    }

    protected fun batch(futures: List<ResultSetFuture>) {
        checkBatchSize()

        batch.addAll(futures)
    }

    private fun checkBatchSize() {
        if (batch.size > MAX_BATCH_SIZE) {
            Futures.allAsList(batch).get()

            batch = newBatch()
        }
    }

    private fun newBatch(): MutableList<ResultSetFuture> {
        return ArrayList(MAX_BATCH_SIZE)
    }

}