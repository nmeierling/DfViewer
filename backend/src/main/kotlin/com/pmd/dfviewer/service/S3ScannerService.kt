package com.pmd.dfviewer.service

import com.pmd.dfviewer.model.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import java.util.concurrent.Executors

@Service
class S3ScannerService(
    private val s3CredentialService: S3CredentialService,
    private val progressService: ProgressService
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val timestampPattern = Regex("timestamp=([^/]+)")
    private val executor = Executors.newCachedThreadPool()

    fun scan(taskId: String, uri: String, maxObjects: Int?) {
        executor.submit {
            try {
                doScan(taskId, uri, maxObjects)
            } catch (e: Exception) {
                log.error("Scan $taskId failed", e)
                progressService.sendScanError(taskId, e.message ?: "Unknown error")
            }
        }
    }

    private fun doScan(taskId: String, uri: String, maxObjects: Int?) {
        val (bucket, prefix) = parseS3Uri(uri)
        val s3 = s3CredentialService.getClient()

        val allFiles = mutableListOf<S3FileEntry>()
        val limitLabel = if (maxObjects != null) " (limit: $maxObjects)" else ""

        log.info("Scan $taskId: scanning s3://$bucket/$prefix$limitLabel")
        progressService.sendScanProgress(taskId, ScanProgress("listing", 0, 0, "Starting scan..."))

        var continuationToken: String? = null
        var totalObjectsScanned = 0
        var pageCount = 0
        var limitReached = false
        do {
            val requestBuilder = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .maxKeys(1000)
            if (continuationToken != null) {
                requestBuilder.continuationToken(continuationToken)
            }

            val response = s3.listObjectsV2(requestBuilder.build())
            pageCount++
            totalObjectsScanned += response.contents().size
            log.info("Scan $taskId page $pageCount: ${response.contents().size} objects (total=$totalObjectsScanned, truncated=${response.isTruncated})")

            response.contents().forEach { obj ->
                val fileType = classifyFile(obj.key())
                if (fileType != FileType.UNKNOWN) {
                    allFiles.add(S3FileEntry(
                        key = obj.key(),
                        uri = "s3://$bucket/${obj.key()}",
                        size = obj.size(),
                        type = fileType,
                        lastModified = obj.lastModified()?.toString()
                    ))
                }
            }

            progressService.sendScanProgress(taskId, ScanProgress(
                "listing", totalObjectsScanned, allFiles.size,
                "Scanned $totalObjectsScanned objects, found ${allFiles.size} data files..."
            ))

            if (maxObjects != null && totalObjectsScanned >= maxObjects) {
                limitReached = true
                log.info("Scan $taskId: max objects limit reached ($maxObjects), stopping")
                break
            }

            continuationToken = if (response.isTruncated) response.nextContinuationToken() else null
        } while (continuationToken != null)

        val truncatedLabel = if (limitReached) " (truncated at $maxObjects)" else ""
        log.info("Scan $taskId: listing complete — ${allFiles.size} data files out of $totalObjectsScanned objects$truncatedLabel")

        progressService.sendScanProgress(taskId, ScanProgress(
            "analyzing", totalObjectsScanned, allFiles.size,
            "Discovering ETL runs in ${allFiles.size} files..."
        ))

        val etlRuns = discoverEtlRuns(allFiles)

        val result = S3ScanResult(
            uri = uri,
            files = allFiles,
            etlRuns = etlRuns
        )

        log.info("Scan $taskId complete: ${allFiles.size} data files, ${etlRuns.size} entity groups, ${etlRuns.sumOf { it.runs.size }} runs$truncatedLabel")

        progressService.sendScanComplete(taskId, result)
    }

    private fun discoverEtlRuns(files: List<S3FileEntry>): List<EtlRunGroup> {
        val runFiles = files.filter { timestampPattern.containsMatchIn(it.key) }
        if (runFiles.isEmpty()) return emptyList()

        val grouped = runFiles.groupBy { file ->
            val key = file.key
            val tsIndex = key.indexOf("timestamp=")
            if (tsIndex > 0) key.substring(0, tsIndex).trimEnd('/') else "unknown"
        }

        return grouped.map { (entityPath, entityFiles) ->
            val byTimestamp = entityFiles.groupBy { file ->
                timestampPattern.find(file.key)?.groupValues?.get(1) ?: "unknown"
            }

            val runs = byTimestamp.map { (ts, tsFiles) ->
                EtlRun(
                    timestamp = ts,
                    files = tsFiles,
                    totalSize = tsFiles.sumOf { it.size }
                )
            }.sortedByDescending { it.timestamp }

            EtlRunGroup(
                entityPath = entityPath,
                runs = runs
            )
        }.sortedBy { it.entityPath }
    }

    private fun classifyFile(key: String): FileType {
        val lower = key.lowercase()
        return when {
            lower.endsWith(".parquet") || lower.endsWith(".snappy.parquet") -> FileType.PARQUET
            lower.endsWith(".csv") || lower.endsWith(".csv.gz") -> FileType.CSV
            else -> FileType.UNKNOWN
        }
    }

    companion object {
        fun parseS3Uri(uri: String): Pair<String, String> {
            val cleaned = uri.removePrefix("s3://").removePrefix("S3://")
            val slashIndex = cleaned.indexOf('/')
            return if (slashIndex < 0) {
                cleaned to ""
            } else {
                cleaned.substring(0, slashIndex) to cleaned.substring(slashIndex + 1)
            }
        }
    }
}
