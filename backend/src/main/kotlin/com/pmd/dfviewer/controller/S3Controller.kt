package com.pmd.dfviewer.controller

import com.pmd.dfviewer.model.S3Credentials
import com.pmd.dfviewer.model.S3ImportRequest
import com.pmd.dfviewer.model.S3ScanResult
import com.pmd.dfviewer.service.S3CredentialService
import com.pmd.dfviewer.service.S3ImportService
import com.pmd.dfviewer.service.S3ScannerService
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

@RestController
@RequestMapping("/api/s3")
class S3Controller(
    private val s3CredentialService: S3CredentialService,
    private val s3ScannerService: S3ScannerService,
    private val s3ImportService: S3ImportService
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

    @GetMapping("/scan", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun scan(@RequestParam uri: String, @RequestParam(required = false) maxObjects: Int?): SseEmitter {
        return s3ScannerService.scan(uri, maxObjects)
    }

    @PostMapping("/import", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun import(@RequestBody request: S3ImportRequest): SseEmitter {
        return s3ImportService.importFiles(request)
    }
}
