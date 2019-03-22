package cc.datafabric.scylladb.pipelines.bulkload

import cc.datafabric.scyllardf.coder.CoderFacade
import cc.datafabric.scyllardf.dao.ICardinalityDAO
import cc.datafabric.scyllardf.dao.IIndexDAO
import cc.datafabric.scyllardf.dao.impl.ScyllaRDFDAOFactory
import com.datastax.driver.core.HostDistance
import com.datastax.driver.core.PoolingOptions
import com.datastax.driver.core.ResultSetFuture
import com.google.common.util.concurrent.Futures
import org.apache.beam.sdk.transforms.DoFn
import org.slf4j.LoggerFactory
import java.net.InetAddress

open class AbstractCassandraExecutor<InputT, OutputT>(
    private val hosts: List<String>,
    private val port: Int,
    private val keyspace: String,
    private val maxRequestsPerConnection: Int
) : DoFn<InputT, OutputT>() {

    companion object {
        private val LOG = LoggerFactory.getLogger(AbstractCassandraExecutor::class.java)
    }

    private lateinit var daoFactory: ScyllaRDFDAOFactory
    protected lateinit var cardinalityDao: ICardinalityDAO
    protected lateinit var indexDAO: IIndexDAO
    protected lateinit var coder: CoderFacade

    private var batch = newBatch()

    @Setup
    public open fun setup() {
        daoFactory = ScyllaRDFDAOFactory.create(hosts.map { InetAddress.getByName(it) }, port, keyspace, PoolingOptions()
            .setMaxRequestsPerConnection(HostDistance.LOCAL, maxRequestsPerConnection)
        )

        coder = CoderFacade
        coder.initialize(daoFactory.getDictionaryDAO())

        cardinalityDao = daoFactory.getCardinalityDAO()
        indexDAO = daoFactory.getIndexDAO()
    }

    @FinishBundle
    public fun finishBundle() {
        checkBatchSize()
    }

    @Teardown
    public fun tearDown() {
        daoFactory.close()
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
            Futures.allAsList(batch).get()

            batch = newBatch()
        }
    }

    private fun newBatch(): MutableList<ResultSetFuture> {
        return ArrayList(maxRequestsPerConnection)
    }

}