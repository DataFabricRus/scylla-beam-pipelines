package cc.datafabric.scylladb.pipelines.io

import cc.datafabric.scylladb.pipelines.coders.RDF4JModelCoder
import cc.datafabric.scylladb.pipelines.transforms.GroupIntoLocalBatches
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.fs.MatchResult
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.Flatten
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.Regex
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionList
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.TupleTagList
import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.rio.Rio
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.StringReader
import java.util.regex.Pattern

object RDF4JIO {

    private val LOG = LoggerFactory.getLogger(RDF4JIO::class.java)

    class Read(private val bufferSize: Long) : PTransform<PCollection<String>, PCollection<Model>>() {

        override fun expand(input: PCollection<String>): PCollection<Model> {
            val separated = input
                .apply(Regex.Split(Pattern.compile("\\s*,\\s*"), false))
                .apply(ParDo.of(SeparateLineAndFileBasedFormats())
                    .withOutputTags(SeparateLineAndFileBasedFormats.LINE_BASED,
                        TupleTagList.of(SeparateLineAndFileBasedFormats.FILE_BASED))
                )

            val lineBased = separated.get(SeparateLineAndFileBasedFormats.LINE_BASED)
                .setCoder(StringUtf8Coder.of())
                .apply(TextIO.readAll())
                .apply(GroupIntoLocalBatches.of(this.bufferSize))
                .apply(ParDo.of(StringsToModel()))
                .setCoder(RDF4JModelCoder.of())

            val fileBased = separated.get(SeparateLineAndFileBasedFormats.FILE_BASED)
                .setCoder(StringUtf8Coder.of())
                .apply<PCollection<MatchResult.Metadata>>(FileIO.matchAll())
                .apply<PCollection<FileIO.ReadableFile>>(FileIO.readMatches())
                .apply(ParDo.of<FileIO.ReadableFile, Model>(FileToModel()))
                .setCoder(RDF4JModelCoder.of())

            return PCollectionList.of<Model>(lineBased).and(fileBased).apply(Flatten.pCollections<Model>())
        }

        private class SeparateLineAndFileBasedFormats : DoFn<String, String>() {

            @ProcessElement
            fun processElement(c: ProcessContext) {
                val filePattern = c.element()
                val format = Rio.getParserFormatForFileName(filePattern)

                if (format.isPresent) {
                    val f = format.get()
                    if (f == RDFFormat.NTRIPLES) {
                        c.output(LINE_BASED, filePattern)
                    } else {
                        c.output(FILE_BASED, filePattern)
                    }
                } else {
                    c.output(FILE_BASED, filePattern)
                }
            }

            companion object {

                val LINE_BASED = TupleTag<String>()
                val FILE_BASED = TupleTag<String>()
            }

        }

        private inner class FileToModel : DoFn<FileIO.ReadableFile, Model>() {

            @ProcessElement
            fun processElement(c: ProcessContext) {
                val file = c.element()
                val resourceId = file.metadata.resourceId()
                val format = Rio.getParserFormatForFileName(resourceId.filename)

                if (!resourceId.isDirectory && format.isPresent) {
                    try {
                        ByteArrayInputStream(file.readFullyAsBytes()).use { bai ->
                            val model = Rio.parse(bai, "", format.get())

                            c.output(model)
                        }
                    } catch (e: IOException) {
                        LOG.error("Failed to read file [{}]!", file.metadata.resourceId().toString())
                    }

                } else {
                    LOG.warn("File [{}] couldn't be matched with any RDF format! Skipping it.",
                        file.metadata.resourceId().toString())
                }
            }

        }

        private inner class StringsToModel : DoFn<Iterable<@JvmSuppressWildcards String>, Model>() {

            @ProcessElement
            @Throws(IOException::class)
            fun processElement(ctx: ProcessContext) {
                val sb = StringBuilder()

                ctx.element().forEach { line -> sb.append(line).append("\n") }

                StringReader(sb.toString()).use { sr ->
                    val model = Rio.parse(sr, "", RDFFormat.NTRIPLES)

                    ctx.output(model)
                }
            }

        }
    }
}
