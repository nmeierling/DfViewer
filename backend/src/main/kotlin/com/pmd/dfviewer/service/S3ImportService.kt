package com.pmd.dfviewer.service

import com.pmd.dfviewer.config.DfViewerProperties
import com.pmd.dfviewer.model.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import java.io.File
import java.io.FileOutputStream
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@Service
class S3ImportService(
    private val s3CredentialService: S3CredentialService,
    private val duckDbService: DuckDbService,
    private val progressService: ProgressService,
    private val properties: DfViewerProperties
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val executor = Executors.newCachedThreadPool()
    private val downloadPool = Executors.newFixedThreadPool(
        Runtime.getRuntime().availableProcessors().coerceIn(4, 16)
    )

    fun importFiles(taskId: String, request: S3ImportRequest) {
        executor.submit {
            try {
                doImport(taskId, request)
            } catch (e: Exception) {
                log.error("Import $taskId failed", e)
                progressService.sendImportProgress(taskId, ImportProgress(
                    phase = "error",
                    fileIndex = 0,
                    totalFiles = request.files?.size ?: 0,
                    fileName = "",
                    bytesDownloaded = 0,
                    bytesTotal = 0,
                    error = e.message,
                    done = true
                ))
            }
        }
    }

    private fun doImport(taskId: String, request: S3ImportRequest) {
        val s3 = s3CredentialService.getClient()
        val datasetId = duckDbService.getNextId()
        val tempDir = File(properties.cacheDir, "tmp/$datasetId").also { it.mkdirs() }
        val totalFiles = request.files.size
        val parallelism = downloadPool.let {
            Runtime.getRuntime().availableProcessors().coerceIn(4, 16)
        }

        log.info("Import $taskId: downloading $totalFiles files with $parallelism parallel threads")

        try {
            // Phase 1: Parallel download
            val completedCount = AtomicInteger(0)
            val totalBytesDownloaded = AtomicLong(0)
            val downloadErrors = mutableListOf<String>()

            progressService.sendImportProgress(taskId, ImportProgress(
                phase = "downloading",
                fileIndex = 0,
                totalFiles = totalFiles,
                fileName = "Starting parallel download ($parallelism threads)...",
                bytesDownloaded = 0,
                bytesTotal = 0
            ))

            val futures = request.files.map { fileUri ->
                downloadPool.submit<Pair<File, FileType>?> {
                    try {
                        val (bucket, key) = S3ScannerService.parseS3Uri(fileUri)
                        val fileType = classifyFileType(key)
                        val fileName = key.substringAfterLast('/')
                        // Use unique name to avoid collisions from same-name files in different paths
                        val localFile = File(tempDir, "${System.nanoTime()}_$fileName")

                        val getResponse = s3.getObject(GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build())

                        var bytesDownloaded = 0L
                        val buffer = ByteArray(1024 * 1024)
                        FileOutputStream(localFile).use { fos ->
                            getResponse.use { input ->
                                var read: Int
                                while (input.read(buffer).also { read = it } != -1) {
                                    fos.write(buffer, 0, read)
                                    bytesDownloaded += read
                                }
                            }
                        }

                        val completed = completedCount.incrementAndGet()
                        totalBytesDownloaded.addAndGet(bytesDownloaded)

                        // Send progress update (throttled — every 10 files or last file)
                        if (completed % 10 == 0 || completed == totalFiles) {
                            progressService.sendImportProgress(taskId, ImportProgress(
                                phase = "downloading",
                                fileIndex = completed,
                                totalFiles = totalFiles,
                                fileName = "$completed / $totalFiles files downloaded",
                                bytesDownloaded = totalBytesDownloaded.get(),
                                bytesTotal = 0
                            ))
                        }

                        localFile to fileType
                    } catch (e: Exception) {
                        synchronized(downloadErrors) {
                            downloadErrors.add("${fileUri}: ${e.message}")
                        }
                        log.error("Import $taskId: failed to download $fileUri", e)
                        null
                    }
                }
            }

            // Wait for all downloads to complete
            val downloadedFiles = futures.mapNotNull { it.get() }
            val totalSizeBytes = totalBytesDownloaded.get()

            if (downloadedFiles.isEmpty()) {
                throw RuntimeException("All downloads failed: ${downloadErrors.joinToString("; ")}")
            }

            if (downloadErrors.isNotEmpty()) {
                log.warn("Import $taskId: ${downloadErrors.size} files failed to download, proceeding with ${downloadedFiles.size}")
            }

            log.info("Import $taskId: downloaded ${downloadedFiles.size}/$totalFiles files (${totalSizeBytes / 1024 / 1024} MB)")

            // Phase 2: DuckDB ingestion — use glob for parquet files (much faster than one-by-one)
            progressService.sendImportProgress(taskId, ImportProgress(
                phase = "ingesting",
                fileIndex = totalFiles,
                totalFiles = totalFiles,
                fileName = "Loading ${downloadedFiles.size} files into database...",
                bytesDownloaded = 0,
                bytesTotal = 0
            ))

            val parquetFiles = downloadedFiles.filter { it.second == FileType.PARQUET }.map { it.first }
            val csvFiles = downloadedFiles.filter { it.second == FileType.CSV }.map { it.first }

            if (parquetFiles.isNotEmpty()) {
                duckDbService.importParquetGlob(tempDir.absolutePath + "/*.parquet", datasetId)
                // Also handle .snappy.parquet if glob didn't catch them
                if (parquetFiles.any { it.name.endsWith(".snappy.parquet") } && !parquetFiles.any { it.name.endsWith(".parquet") && !it.name.endsWith(".snappy.parquet") }) {
                    // All files are .snappy.parquet, glob already covered them via *.parquet
                }
            }

            if (csvFiles.isNotEmpty()) {
                for ((index, file) in csvFiles.withIndex()) {
                    if (index == 0 && parquetFiles.isEmpty()) {
                        duckDbService.importCsvFile(file.absolutePath, datasetId)
                    } else {
                        duckDbService.appendCsvFile(file.absolutePath, datasetId)
                    }
                }
            }

            val schema = duckDbService.getSchema(datasetId)
            val rowCount = duckDbService.getRowCount(datasetId)

            val sourceType = when {
                parquetFiles.isNotEmpty() -> SourceType.S3_PARQUET
                csvFiles.isNotEmpty() -> SourceType.S3_CSV
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
                sizeBytes = totalSizeBytes,
                schema = schema
            )

            progressService.sendImportProgress(taskId, ImportProgress(
                phase = "done",
                fileIndex = totalFiles,
                totalFiles = totalFiles,
                fileName = "",
                bytesDownloaded = 0,
                bytesTotal = 0,
                datasetId = datasetId,
                done = true
            ))

            duckDbService.checkpoint()
            log.info("Import $taskId complete: dataset $datasetId (${request.name}), $rowCount rows, ${totalSizeBytes / 1024 / 1024} MB")

        } finally {
            tempDir.deleteRecursively()
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
}
