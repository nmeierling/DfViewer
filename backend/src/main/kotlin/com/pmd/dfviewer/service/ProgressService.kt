package com.pmd.dfviewer.service

import com.pmd.dfviewer.model.ImportProgress
import com.pmd.dfviewer.model.ScanProgress
import com.pmd.dfviewer.model.S3ScanResult
import org.slf4j.LoggerFactory
import org.springframework.messaging.simp.SimpMessagingTemplate
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap

@Service
class ProgressService(
    private val messagingTemplate: SimpMessagingTemplate
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /** Stores completed scan results for REST retrieval (avoids large WS frames) */
    private val scanResults = ConcurrentHashMap<String, S3ScanResult>()

    fun sendScanProgress(taskId: String, progress: ScanProgress) {
        messagingTemplate.convertAndSend("/topic/scan/$taskId", mapOf(
            "type" to "progress",
            "data" to progress
        ))
    }

    /** Store result for REST retrieval, send lightweight completion signal over WS */
    fun sendScanComplete(taskId: String, result: S3ScanResult) {
        scanResults[taskId] = result
        val summary = mapOf(
            "fileCount" to result.files.size,
            "etlGroupCount" to result.etlRuns.size,
            "runCount" to result.etlRuns.sumOf { it.runs.size }
        )
        messagingTemplate.convertAndSend("/topic/scan/$taskId", mapOf(
            "type" to "complete",
            "data" to summary
        ))
    }

    fun getScanResult(taskId: String): S3ScanResult? = scanResults.remove(taskId)

    fun sendScanError(taskId: String, error: String) {
        messagingTemplate.convertAndSend("/topic/scan/$taskId", mapOf(
            "type" to "error",
            "data" to mapOf("error" to error)
        ))
    }

    fun sendImportProgress(taskId: String, progress: ImportProgress) {
        messagingTemplate.convertAndSend("/topic/import/$taskId", mapOf(
            "type" to "progress",
            "data" to progress
        ))
    }
}
