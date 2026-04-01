package com.pmd.dfviewer.controller

import com.pmd.dfviewer.config.DfViewerProperties
import com.pmd.dfviewer.service.DuckDbService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import java.io.File

@RestController
class HealthController(
    private val properties: DfViewerProperties,
    private val duckDbService: DuckDbService
) {

    @GetMapping("/api/health")
    fun health(): Map<String, Any> {
        val dbFile = File(properties.duckdbPath)
        val dbSizeBytes = if (dbFile.exists()) dbFile.length() else 0L
        return mapOf(
            "status" to "ok",
            "duckdbSizeBytes" to dbSizeBytes
        )
    }

    @PostMapping("/api/compact")
    fun compact(): Map<String, Any> {
        val before = File(properties.duckdbPath).length()
        duckDbService.checkpoint()
        val after = File(properties.duckdbPath).length()
        return mapOf(
            "beforeBytes" to before,
            "afterBytes" to after,
            "savedBytes" to (before - after)
        )
    }
}
