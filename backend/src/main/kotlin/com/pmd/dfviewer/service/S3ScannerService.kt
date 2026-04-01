package com.pmd.dfviewer.service

import com.pmd.dfviewer.model.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import software.amazon.awssdk.services.s3.model.CommonPrefix
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Future

@Service
class S3ScannerService(
    private val s3CredentialService: S3CredentialService,
    private val progressService: ProgressService,
    private val scanCacheService: ScanCacheService
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val timestampPattern = Regex("timestamp=([^/]+)")
    private val executor = Executors.newCachedThreadPool()
    private val scanPool = Executors.newFixedThreadPool(
        Runtime.getRuntime().availableProcessors().coerceIn(4, 16)
    )

    fun scan(taskId: String, uri: String, maxObjects: Int?, forceRescan: Boolean = false) {
        executor.submit {
            try {
                if (!forceRescan && maxObjects == null) {
                    val cached = scanCacheService.getCached(uri)
                    if (cached != null) {
                        log.info("Scan $taskId: serving cached result for $uri")
                        progressService.sendScanProgress(taskId, ScanProgress("listing", 0, cached.files.size, "Loaded from cache"))
                        progressService.sendScanComplete(taskId, cached)
                        return@submit
                    }
                }
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
        val limitLabel = if (maxObjects != null) " (limit: $maxObjects)" else ""

        log.info("Scan $taskId: scanning s3://$bucket/$prefix$limitLabel")
        progressService.sendScanProgress(taskId, ScanProgress("listing", 0, 0, "Discovering sub-prefixes..."))

        // Step 1: discover immediate sub-prefixes using delimiter
        val subPrefixes = discoverSubPrefixes(bucket, prefix)

        if (subPrefixes.size > 1) {
            log.info("Scan $taskId: found ${subPrefixes.size} sub-prefixes, scanning in parallel")
            doParallelScan(taskId, uri, bucket, subPrefixes, maxObjects)
        } else {
            log.info("Scan $taskId: single prefix, scanning sequentially")
            doSequentialScan(taskId, uri, bucket, prefix, maxObjects)
        }
    }

    private fun discoverSubPrefixes(bucket: String, prefix: String): List<String> {
        val s3 = s3CredentialService.getClient()
        val subPrefixes = mutableListOf<String>()

        var continuationToken: String? = null
        do {
            val builder = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .delimiter("/")
                .maxKeys(1000)
            if (continuationToken != null) builder.continuationToken(continuationToken)

            val response = s3.listObjectsV2(builder.build())
            subPrefixes.addAll(response.commonPrefixes().map { it.prefix() })
            continuationToken = if (response.isTruncated) response.nextContinuationToken() else null
        } while (continuationToken != null)

        // If there are also direct files at this level, include the prefix itself
        return if (subPrefixes.isEmpty()) listOf(prefix) else subPrefixes
    }

    private fun doParallelScan(taskId: String, uri: String, bucket: String, subPrefixes: List<String>, maxObjects: Int?) {
        val allFiles = ConcurrentLinkedQueue<S3FileEntry>()
        val totalScanned = AtomicInteger(0)
        val completedPrefixes = AtomicInteger(0)
        val totalPrefixes = subPrefixes.size
        val parallelism = scanPool.let { Runtime.getRuntime().availableProcessors().coerceIn(4, 16) }

        progressService.sendScanProgress(taskId, ScanProgress(
            "listing", 0, 0,
            "Scanning $totalPrefixes sub-prefixes with $parallelism threads..."
        ))

        val futures: List<Future<*>> = subPrefixes.map { subPrefix ->
            scanPool.submit {
                val s3 = s3CredentialService.getClient()
                var continuationToken: String? = null

                do {
                    if (maxObjects != null && totalScanned.get() >= maxObjects) break

                    val builder = ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .prefix(subPrefix)
                        .maxKeys(1000)
                    if (continuationToken != null) builder.continuationToken(continuationToken)

                    val response = s3.listObjectsV2(builder.build())
                    val scanned = totalScanned.addAndGet(response.contents().size)

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

                    continuationToken = if (response.isTruncated) response.nextContinuationToken() else null
                } while (continuationToken != null)

                val done = completedPrefixes.incrementAndGet()
                progressService.sendScanProgress(taskId, ScanProgress(
                    "listing", totalScanned.get(), allFiles.size,
                    "Scanned $done/$totalPrefixes prefixes — ${totalScanned.get()} objects, ${allFiles.size} data files"
                ))
            }
        }

        // Wait for all
        futures.forEach { it.get() }

        val fileList = allFiles.toList()
        finalizeScan(taskId, uri, fileList, totalScanned.get(), maxObjects)
    }

    private fun doSequentialScan(taskId: String, uri: String, bucket: String, prefix: String, maxObjects: Int?) {
        val s3 = s3CredentialService.getClient()
        val allFiles = mutableListOf<S3FileEntry>()

        var continuationToken: String? = null
        var totalObjectsScanned = 0
        var limitReached = false

        do {
            val builder = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .maxKeys(1000)
            if (continuationToken != null) builder.continuationToken(continuationToken)

            val response = s3.listObjectsV2(builder.build())
            totalObjectsScanned += response.contents().size

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
                break
            }

            continuationToken = if (response.isTruncated) response.nextContinuationToken() else null
        } while (continuationToken != null)

        finalizeScan(taskId, uri, allFiles, totalObjectsScanned, if (limitReached) maxObjects else null)
    }

    private fun finalizeScan(taskId: String, uri: String, files: List<S3FileEntry>, totalObjectsScanned: Int, maxObjects: Int?) {
        val truncatedLabel = if (maxObjects != null && totalObjectsScanned >= maxObjects) " (truncated at $maxObjects)" else ""
        log.info("Scan $taskId: listing complete — ${files.size} data files out of $totalObjectsScanned objects$truncatedLabel")

        progressService.sendScanProgress(taskId, ScanProgress(
            "analyzing", totalObjectsScanned, files.size,
            "Discovering ETL runs in ${files.size} files..."
        ))

        val etlRuns = discoverEtlRuns(files)

        val result = S3ScanResult(
            uri = uri,
            files = files,
            etlRuns = etlRuns
        )

        // Cache result (only for full scans without maxObjects limit)
        if (maxObjects == null) {
            scanCacheService.store(uri, result)
        }

        log.info("Scan $taskId complete: ${files.size} data files, ${etlRuns.size} entity groups, ${etlRuns.sumOf { it.runs.size }} runs$truncatedLabel")
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
