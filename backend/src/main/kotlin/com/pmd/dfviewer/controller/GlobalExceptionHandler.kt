package com.pmd.dfviewer.controller

import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import software.amazon.awssdk.services.s3.model.S3Exception

@RestControllerAdvice
class GlobalExceptionHandler {

    private val log = LoggerFactory.getLogger(javaClass)

    @ExceptionHandler(S3Exception::class)
    fun handleS3Exception(ex: S3Exception): ResponseEntity<Map<String, Any>> {
        log.error("S3 error: ${ex.awsErrorDetails()?.errorCode()} - ${ex.message}")

        val errorCode = ex.awsErrorDetails()?.errorCode() ?: "Unknown"
        val isExpired = errorCode in listOf("ExpiredToken", "ExpiredTokenException", "RequestExpired") ||
            ex.message?.contains("expired", ignoreCase = true) == true ||
            ex.message?.contains("token", ignoreCase = true) == true && ex.statusCode() == 403

        val status = if (isExpired) HttpStatus.UNAUTHORIZED else HttpStatus.BAD_REQUEST

        return ResponseEntity.status(status).body(mapOf(
            "error" to if (isExpired) "TOKEN_EXPIRED" else "S3_ERROR",
            "message" to if (isExpired) "AWS credentials have expired. Please reconfigure." else (ex.awsErrorDetails()?.errorMessage() ?: ex.message ?: "S3 error"),
            "code" to errorCode
        ))
    }

    @ExceptionHandler(IllegalStateException::class)
    fun handleIllegalState(ex: IllegalStateException): ResponseEntity<Map<String, Any>> {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(mapOf(
            "error" to "CONFIGURATION_ERROR",
            "message" to (ex.message ?: "Configuration error")
        ))
    }
}
