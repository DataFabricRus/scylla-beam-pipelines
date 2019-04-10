package cc.datafabric.scylladb.pipelines.io

import cc.datafabric.scyllardf.coder.CoderFacade
import cc.datafabric.scyllardf.coder.ICoderFacade
import cc.datafabric.scyllardf.dao.ICardinalityDAO
import cc.datafabric.scyllardf.dao.IIndexDAO
import cc.datafabric.scyllardf.dao.impl.ScyllaRDFDAOFactory
import com.datastax.driver.core.HostDistance
import com.datastax.driver.core.PoolingOptions
import com.datastax.driver.core.ResultSetFuture
import org.apache.beam.sdk.transforms.DoFn
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger

open class AbstractCassandraExecutor<InputT, OutputT>(
    private val hosts: List<String>,
    private val port: Int,
    private val keyspace: String,
    private val maxRequestsPerConnection: Int
) : DoFn<InputT, OutputT>() {

    companion object {
        private val LOG = LoggerFactory.getLogger(AbstractCassandraExecutor::class.java)

        private val numExecutors = AtomicInteger(0)

        @Volatile
        private lateinit var daoFactory: ScyllaRDFDAOFactory
    }

    protected lateinit var coder: ICoderFacade
    protected lateinit var cardinalityDao: ICardinalityDAO
    protected lateinit var indexDAO: IIndexDAO

    private var batch = newBatch()

    @Setup
    public open fun setup() {
        synchronized(AbstractCassandraExecutor::class.java) {
            if (numExecutors.getAndIncrement() == 0) {
                daoFactory = ScyllaRDFDAOFactory.create(
                    hosts.map { InetAddress.getByName(it) },
                    port,
                    keyspace,
                    PoolingOptions()
                        .setMaxConnectionsPerHost(HostDistance.LOCAL, 2)
                        .setMaxRequestsPerConnection(HostDistance.LOCAL, maxRequestsPerConnection)
                )
            }
        }

        coder = CoderFacade()
        (coder as CoderFacade).initialize(daoFactory.getDictionaryDAO())

        cardinalityDao = daoFactory.getCardinalityDAO()
        indexDAO = daoFactory.getIndexDAO()
    }

    @FinishBundle
    public fun finishBundle() {
        checkBatchSize()
    }

    @Teardown
    public fun tearDown() {
        synchronized(AbstractCassandraExecutor::class.java) {
            if (numExecutors.decrementAndGet() == 0) {
                daoFactory.close()
            }
        }
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
        if (batch.size > maxRequestsPerConnection) {
            val start = System.currentTimeMillis()

            batch.forEach {
                try {
                    it.getUninterruptibly()
                } catch (t: Throwable) {
                    LOG.warn(t.message, t)
                }
            }

            batch.clear()

            LOG.info("Batch completed in {} ms", System.currentTimeMillis() - start)
        }
    }

    private fun newBatch(): MutableList<ResultSetFuture> {
        return ArrayList(maxRequestsPerConnection + maxRequestsPerConnection / 10)
    }

}