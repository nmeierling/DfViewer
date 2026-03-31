package com.pmd.dfviewer.controller

import com.pmd.dfviewer.model.S3Credentials
import com.pmd.dfviewer.model.S3ImportRequest

import com.pmd.dfviewer.model.S3ScanResult
import com.pmd.dfviewer.service.S3CredentialService
import com.pmd.dfviewer.service.S3ImportService
import com.pmd.dfviewer.service.S3ScannerService
import com.pmd.dfviewer.service.ProgressService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

data class ScanRequest(
    val uri: String,
    val maxObjects: Int? = null,
    val taskId: String
)

data class ImportRequestWithTask(
    val files: List<String>,
    val name: String,
    val entityPath: String? = null,
    val runTimestamp: String? = null,
    val taskId: String
)

@RestController
@RequestMapping("/api/s3")
class S3Controller(
    private val s3CredentialService: S3CredentialService,
    private val s3ScannerService: S3ScannerService,
    private val s3ImportService: S3ImportService,
    private val progressService: ProgressService
) {

    @PostMapping("/configure")
    fun configure(@RequestBody credentials: S3Credentials): ResponseEntity<Map<String, Any>> {
        s3CredentialService.configure(credentials)
        return ResponseEntity.ok(mapOf("configured" to true))
    }

    @GetMapping("/status")
    fun status(): ResponseEntity<Map<String, Any>> {
        return ResponseEntity.ok(mapOf(
            "configured" to s3CredentialService.isConfigured(),
            "region" to (s3CredentialService.getCredentials()?.region ?: "not set")
        ))
    }

    @PostMapping("/scan")
    fun scan(@RequestBody request: ScanRequest): ResponseEntity<Map<String, String>> {
        s3ScannerService.scan(request.taskId, request.uri, request.maxObjects)
        return ResponseEntity.ok(mapOf("taskId" to request.taskId))
    }

    @GetMapping("/scan/{taskId}/result")
    fun scanResult(@PathVariable taskId: String): ResponseEntity<S3ScanResult> {
        val result = progressService.getScanResult(taskId)
            ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(result)
    }

    @PostMapping("/import")
    fun import(@RequestBody request: ImportRequestWithTask): ResponseEntity<Map<String, String>> {
        val importRequest = S3ImportRequest(
            files = request.files,
            name = request.name,
            entityPath = request.entityPath,
            runTimestamp = request.runTimestamp
        )
        s3ImportService.importFiles(request.taskId, importRequest)
        return ResponseEntity.ok(mapOf("taskId" to request.taskId))
    }
}
