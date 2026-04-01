package com.pmd.dfviewer.controller

import com.pmd.dfviewer.model.CompareRequest
import com.pmd.dfviewer.model.ComparisonSummary
import com.pmd.dfviewer.model.DataPage
import com.pmd.dfviewer.service.ColumnChangeSummary
import com.pmd.dfviewer.service.ComparisonService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/compare")
class ComparisonController(
    private val comparisonService: ComparisonService
) {

    @PostMapping
    fun compare(@RequestBody request: CompareRequest): ResponseEntity<ComparisonSummary> {
        return ResponseEntity.ok(comparisonService.compare(request))
    }

    @GetMapping("/{leftId}/{rightId}/added")
    fun getAdded(
        @PathVariable leftId: Long,
        @PathVariable rightId: Long,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "100") size: Int
    ): ResponseEntity<DataPage> {
        return ResponseEntity.ok(comparisonService.getAdded(leftId, rightId, page, size))
    }

    @GetMapping("/{leftId}/{rightId}/removed")
    fun getRemoved(
        @PathVariable leftId: Long,
        @PathVariable rightId: Long,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "100") size: Int
    ): ResponseEntity<DataPage> {
        return ResponseEntity.ok(comparisonService.getRemoved(leftId, rightId, page, size))
    }

    @GetMapping("/{leftId}/{rightId}/changed")
    fun getChanged(
        @PathVariable leftId: Long,
        @PathVariable rightId: Long,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "100") size: Int
    ): ResponseEntity<DataPage> {
        return ResponseEntity.ok(comparisonService.getChanged(leftId, rightId, page, size))
    }

    @GetMapping("/{leftId}/{rightId}/column-changes")
    fun getColumnChanges(
        @PathVariable leftId: Long,
        @PathVariable rightId: Long
    ): ResponseEntity<List<ColumnChangeSummary>> {
        return ResponseEntity.ok(comparisonService.getColumnChanges(leftId, rightId))
    }

    @GetMapping("/{leftId}/{rightId}/column/{column}")
    fun getColumnChangeData(
        @PathVariable leftId: Long,
        @PathVariable rightId: Long,
        @PathVariable column: String,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "100") size: Int
    ): ResponseEntity<DataPage> {
        return ResponseEntity.ok(comparisonService.getColumnChangeData(leftId, rightId, column, page, size))
    }
}
