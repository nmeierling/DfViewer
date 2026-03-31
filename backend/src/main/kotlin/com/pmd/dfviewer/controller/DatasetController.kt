package com.pmd.dfviewer.controller

import com.pmd.dfviewer.model.DataPage
import com.pmd.dfviewer.model.Dataset
import com.pmd.dfviewer.model.ColumnInfo
import com.pmd.dfviewer.service.DuckDbService
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/datasets")
class DatasetController(
    private val duckDbService: DuckDbService,
    private val objectMapper: ObjectMapper
) {

    @GetMapping
    fun listDatasets(): List<Dataset> = duckDbService.listDatasets()

    @GetMapping("/{id}")
    fun getDataset(@PathVariable id: Long): ResponseEntity<Dataset> {
        val dataset = duckDbService.getDataset(id) ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(dataset)
    }

    @GetMapping("/{id}/schema")
    fun getSchema(@PathVariable id: Long): ResponseEntity<List<ColumnInfo>> {
        duckDbService.getDataset(id) ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(duckDbService.getSchema(id))
    }

    @GetMapping("/{id}/data")
    fun getData(
        @PathVariable id: Long,
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "100") size: Int,
        @RequestParam(required = false) sortField: String?,
        @RequestParam(required = false) sortOrder: String?,
        @RequestParam(required = false) filters: String?
    ): ResponseEntity<DataPage> {
        duckDbService.getDataset(id) ?: return ResponseEntity.notFound().build()
        val filterMap: Map<String, String>? = filters?.let { objectMapper.readValue(it) }
        return ResponseEntity.ok(duckDbService.queryData(id, page, size, sortField, sortOrder, filterMap))
    }

    @DeleteMapping("/{id}")
    fun deleteDataset(@PathVariable id: Long): ResponseEntity<Void> {
        duckDbService.getDataset(id) ?: return ResponseEntity.notFound().build()
        duckDbService.deleteDataset(id)
        return ResponseEntity.noContent().build()
    }
}
