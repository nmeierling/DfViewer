package com.pmd.dfviewer.service

import com.pmd.dfviewer.config.DfViewerProperties
import com.pmd.dfviewer.model.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import java.io.File
import java.io.FileOutputStream
import java.util.concurrent.Executors

@Service
class S3ImportService(
    private val s3CredentialService: S3CredentialService,
    private val duckDbService: DuckDbService,
    private val progressService: ProgressService,
    private val properties: DfViewerProperties
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val executor = Executors.newCachedThreadPool()

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

        try {
            val downloadedFiles = mutableListOf<Pair<File, FileType>>()

            for ((index, fileUri) in request.files.withIndex()) {
                val (bucket, key) = S3ScannerService.parseS3Uri(fileUri)
                val fileType = classifyFileType(key)
                val fileName = key.substringAfterLast('/')
                val localFile = File(tempDir, fileName)

                val headResponse = s3.headObject(HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build())
                val totalBytes = headResponse.contentLength()

                progressService.sendImportProgress(taskId, ImportProgress(
                    phase = "downloading",
                    fileIndex = index + 1,
                    totalFiles = request.files.size,
                    fileName = fileName,
                    bytesDownloaded = 0,
                    bytesTotal = totalBytes
                ))

                val getResponse = s3.getObject(GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build())

                var bytesDownloaded = 0L
                val buffer = ByteArray(8 * 1024 * 1024)
                FileOutputStream(localFile).use { fos ->
                    getResponse.use { input ->
                        var read: Int
                        while (input.read(buffer).also { read = it } != -1) {
                            fos.write(buffer, 0, read)
                            bytesDownloaded += read
                            progressService.sendImportProgress(taskId, ImportProgress(
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
                log.info("Import $taskId: downloaded $fileUri (${localFile.length()} bytes)")
            }

            progressService.sendImportProgress(taskId, ImportProgress(
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

            val schema = duckDbService.getSchema(datasetId)
            val rowCount = duckDbService.getRowCount(datasetId)

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

            progressService.sendImportProgress(taskId, ImportProgress(
                phase = "done",
                fileIndex = request.files.size,
                totalFiles = request.files.size,
                fileName = "",
                bytesDownloaded = 0,
                bytesTotal = 0,
                datasetId = datasetId,
                done = true
            ))

            log.info("Import $taskId complete: dataset $datasetId (${request.name}), $rowCount rows")

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
