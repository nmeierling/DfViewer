package com.pmd.dfviewer.service

import com.pmd.dfviewer.config.DfViewerProperties
import com.pmd.dfviewer.model.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import java.io.File
import java.io.FileOutputStream
import java.util.concurrent.Executors

@Service
class S3ImportService(
    private val s3CredentialService: S3CredentialService,
    private val duckDbService: DuckDbService,
    private val properties: DfViewerProperties
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val executor = Executors.newCachedThreadPool()

    fun importFiles(request: S3ImportRequest): SseEmitter {
        val emitter = SseEmitter(0L) // no timeout

        executor.submit {
            try {
                doImport(request, emitter)
            } catch (e: Exception) {
                log.error("Import failed", e)
                sendProgress(emitter, ImportProgress(
                    phase = "error",
                    fileIndex = 0,
                    totalFiles = request.files.size,
                    fileName = "",
                    bytesDownloaded = 0,
                    bytesTotal = 0,
                    error = e.message,
                    done = true
                ))
                emitter.complete()
            }
        }

        return emitter
    }

    private fun doImport(request: S3ImportRequest, emitter: SseEmitter) {
        val s3 = s3CredentialService.getClient()
        val datasetId = duckDbService.getNextId()
        val tempDir = File(properties.cacheDir, "tmp/$datasetId").also { it.mkdirs() }

        try {
            val downloadedFiles = mutableListOf<Pair<File, FileType>>()

            for ((index, fileUri) in request.files.withIndex()) {
                val (bucket, key) = S3ScannerService.parseS3Uri(fileUri)
                val fileType = classifyFileType(key)
                val fileName = key.substringAfterLast('/')
                val localFile = File(tempDir, fileName)

                // Get file size
                val headResponse = s3.headObject(HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build())
                val totalBytes = headResponse.contentLength()

                sendProgress(emitter, ImportProgress(
                    phase = "downloading",
                    fileIndex = index + 1,
                    totalFiles = request.files.size,
                    fileName = fileName,
                    bytesDownloaded = 0,
                    bytesTotal = totalBytes
                ))

                // Download with progress
                val getResponse = s3.getObject(GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build())

                var bytesDownloaded = 0L
                val buffer = ByteArray(8 * 1024 * 1024) // 8MB buffer
                FileOutputStream(localFile).use { fos ->
                    getResponse.use { input ->
                        var read: Int
                        while (input.read(buffer).also { read = it } != -1) {
                            fos.write(buffer, 0, read)
                            bytesDownloaded += read
                            sendProgress(emitter, ImportProgress(
                                phase = "downloading",
                                fileIndex = index + 1,
                                totalFiles = request.files.size,
                                fileName = fileName,
                                bytesDownloaded = bytesDownloaded,
                                bytesTotal = totalBytes
                            ))
                        }
                    }
                }

                downloadedFiles.add(localFile to fileType)
                log.info("Downloaded $fileUri -> ${localFile.absolutePath} (${localFile.length()} bytes)")
            }

            // Ingest into DuckDB
            sendProgress(emitter, ImportProgress(
                phase = "ingesting",
                fileIndex = request.files.size,
                totalFiles = request.files.size,
                fileName = "Loading into database...",
                bytesDownloaded = 0,
                bytesTotal = 0
            ))

            for ((index, pair) in downloadedFiles.withIndex()) {
                val (file, fileType) = pair
                if (index == 0) {
                    when (fileType) {
                        FileType.PARQUET -> duckDbService.importParquetFile(file.absolutePath, datasetId)
                        FileType.CSV -> duckDbService.importCsvFile(file.absolutePath, datasetId)
                        else -> throw IllegalArgumentException("Unsupported file type: $fileType")
                    }
                } else {
                    when (fileType) {
                        FileType.PARQUET -> duckDbService.appendParquetFile(file.absolutePath, datasetId)
                        FileType.CSV -> duckDbService.appendCsvFile(file.absolutePath, datasetId)
                        else -> throw IllegalArgumentException("Unsupported file type: $fileType")
                    }
                }
            }

            // Get schema and row count
            val schema = duckDbService.getSchema(datasetId)
            val rowCount = duckDbService.getRowCount(datasetId)

            // Determine source type
            val firstFileType = downloadedFiles.first().second
            val sourceType = when (firstFileType) {
                FileType.PARQUET -> SourceType.S3_PARQUET
                FileType.CSV -> SourceType.S3_CSV
                else -> SourceType.S3_PARQUET
            }

            duckDbService.registerDataset(
                id = datasetId,
                name = request.name,
                sourceUri = if (request.files.size == 1) request.files.first() else "${request.files.size} files",
                sourceType = sourceType,
                entityPath = request.entityPath,
                runTimestamp = request.runTimestamp,
                rowCount = rowCount,
                schema = schema
            )

            sendProgress(emitter, ImportProgress(
                phase = "done",
                fileIndex = request.files.size,
                totalFiles = request.files.size,
                fileName = "",
                bytesDownloaded = 0,
                bytesTotal = 0,
                datasetId = datasetId,
                done = true
            ))

            log.info("Import complete: dataset $datasetId (${request.name}), $rowCount rows")

        } finally {
            // Clean up temp files
            tempDir.deleteRecursively()
            emitter.complete()
        }
    }

    private fun classifyFileType(key: String): FileType {
        val lower = key.lowercase()
        return when {
            lower.endsWith(".parquet") || lower.endsWith(".snappy.parquet") -> FileType.PARQUET
            lower.endsWith(".csv") || lower.endsWith(".csv.gz") -> FileType.CSV
            else -> FileType.UNKNOWN
        }
    }

    private fun sendProgress(emitter: SseEmitter, progress: ImportProgress) {
        try {
            emitter.send(SseEmitter.event().name("progress").data(progress))
        } catch (e: Exception) {
            log.debug("Failed to send SSE progress: ${e.message}")
        }
    }
}
