package com.pmd.dfviewer.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pmd.dfviewer.config.DfViewerProperties
import com.pmd.dfviewer.model.S3ScanResult
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.time.Instant
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

data class ScanCacheEntry(
    val uri: String,
    val scannedAt: Instant,
    val fileCount: Int,
    val etlGroupCount: Int,
    val runCount: Int
)

@Service
class ScanCacheService(
    private val properties: DfViewerProperties,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val cacheDir get() = File(properties.cacheDir, "scan-cache").also { it.mkdirs() }

    private fun cacheFile(uri: String): File {
        val hash = uri.hashCode().toUInt().toString(16)
        return File(cacheDir, "$hash.json.gz")
    }

    private fun metaFile(uri: String): File {
        val hash = uri.hashCode().toUInt().toString(16)
        return File(cacheDir, "$hash.meta.json")
    }

    fun getCached(uri: String): S3ScanResult? {
        val file = cacheFile(uri)
        if (!file.exists()) return null
        return try {
            GZIPInputStream(FileInputStream(file)).use { gis ->
                objectMapper.readValue(gis, S3ScanResult::class.java)
            }
        } catch (e: Exception) {
            log.warn("Failed to read scan cache for $uri: ${e.message}")
            null
        }
    }

    fun store(uri: String, result: S3ScanResult) {
        try {
            GZIPOutputStream(FileOutputStream(cacheFile(uri))).use { gos ->
                objectMapper.writeValue(gos, result)
            }
            val meta = ScanCacheEntry(
                uri = uri,
                scannedAt = Instant.now(),
                fileCount = result.files.size,
                etlGroupCount = result.etlRuns.size,
                runCount = result.etlRuns.sumOf { it.runs.size }
            )
            objectMapper.writeValue(metaFile(uri), meta)
            log.info("Cached scan result for $uri (${result.files.size} files)")
        } catch (e: Exception) {
            log.warn("Failed to cache scan result for $uri: ${e.message}")
        }
    }

    fun listCached(): List<ScanCacheEntry> {
        return cacheDir.listFiles { _, name -> name.endsWith(".meta.json") }
            ?.mapNotNull { file ->
                try {
                    objectMapper.readValue(file, ScanCacheEntry::class.java)
                } catch (_: Exception) {
                    null
                }
            }
            ?.sortedByDescending { it.scannedAt }
            ?: emptyList()
    }

    fun invalidate(uri: String) {
        cacheFile(uri).delete()
        metaFile(uri).delete()
        log.info("Invalidated scan cache for $uri")
    }
}
