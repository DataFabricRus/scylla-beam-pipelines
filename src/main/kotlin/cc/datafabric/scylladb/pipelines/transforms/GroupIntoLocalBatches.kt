package cc.datafabric.scylladb.pipelines.transforms

import com.google.common.base.Preconditions.checkArgument
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.GlobalWindow
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Instant
import org.slf4j.LoggerFactory
import java.util.ArrayList

class GroupIntoLocalBatches<T> private constructor(private val batchSize: Long) : PTransform<PCollection<T>, PCollection<Iterable<T>>>() {

    override fun expand(input: PCollection<T>): PCollection<Iterable<T>> {
        return input.apply(ParDo.of(object : DoFn<T, Iterable<@JvmSuppressWildcards T>>() {

            @Transient
            private var batch: MutableList<T> = ArrayList()

            @DoFn.StartBundle
            fun startBundle() {
                LOG.debug("Start up batch")
                batch = ArrayList()
            }

            @DoFn.ProcessElement
            fun processElement(c: ProcessContext) {
                val element = c.element()
                checkArgument(element != null, "Can't batch nulls!")

                batch.add(element)

                if (batch.size >= batchSize) {
                    LOG.debug("Flush batch on the threshold")

                    val outputBatch = batch
                    batch = ArrayList()

                    c.outputWithTimestamp(outputBatch, Instant.now())
                }
            }

            @FinishBundle
            fun finishBundle(ctx: FinishBundleContext) {
                LOG.debug("Flush batch on finished bundle")

                val outputBatch = batch
                batch = ArrayList()

                ctx.output(outputBatch, Instant.now(), GlobalWindow.INSTANCE)
            }

        }))
    }

    companion object {

        private val LOG = LoggerFactory.getLogger(GroupIntoLocalBatches::class.java)

        fun <T> of(batchSize: Long): GroupIntoLocalBatches<T> {
            return GroupIntoLocalBatches(batchSize)
        }
    }
}
