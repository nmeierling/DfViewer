package com.pmd.dfviewer.controller

import com.pmd.dfviewer.config.DfViewerProperties
import com.pmd.dfviewer.model.*
import com.pmd.dfviewer.service.DuckDbService
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.multipart.MultipartFile
import java.io.File

@RestController
@RequestMapping("/api/datasets")
class DatasetController(
    private val duckDbService: DuckDbService,
    private val objectMapper: ObjectMapper,
    private val properties: DfViewerProperties
) {
    private val log = LoggerFactory.getLogger(javaClass)

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

    @GetMapping("/{id}/hidden-columns")
    fun getHiddenColumns(@PathVariable id: Long): ResponseEntity<List<String>> {
        return ResponseEntity.ok(duckDbService.getHiddenColumns(id))
    }

    @PutMapping("/{id}/hidden-columns")
    fun setHiddenColumns(@PathVariable id: Long, @RequestBody hiddenColumns: List<String>): ResponseEntity<Void> {
        duckDbService.setHiddenColumns(id, hiddenColumns)
        return ResponseEntity.noContent().build()
    }

    @GetMapping("/{id}/column-order")
    fun getColumnOrder(@PathVariable id: Long): ResponseEntity<List<String>> {
        return ResponseEntity.ok(duckDbService.getColumnOrder(id))
    }

    @PutMapping("/{id}/column-order")
    fun setColumnOrder(@PathVariable id: Long, @RequestBody columnOrder: List<String>): ResponseEntity<Void> {
        duckDbService.setColumnOrder(id, columnOrder)
        return ResponseEntity.noContent().build()
    }

    @GetMapping("/{id}/column-widths")
    fun getColumnWidths(@PathVariable id: Long): ResponseEntity<Map<String, Int>> {
        return ResponseEntity.ok(duckDbService.getColumnWidths(id))
    }

    @PutMapping("/{id}/column-widths")
    fun setColumnWidths(@PathVariable id: Long, @RequestBody widths: Map<String, Int>): ResponseEntity<Void> {
        duckDbService.setColumnWidths(id, widths)
        return ResponseEntity.noContent().build()
    }

    @GetMapping("/{id}/distinct/{column}")
    fun getDistinctValues(
        @PathVariable id: Long,
        @PathVariable column: String,
        @RequestParam(required = false) filters: String?,
        @RequestParam(defaultValue = "500") limit: Int
    ): ResponseEntity<List<Map<String, Any?>>> {
        duckDbService.getDataset(id) ?: return ResponseEntity.notFound().build()
        val filterMap: Map<String, String>? = filters?.let { objectMapper.readValue(it) }
        return ResponseEntity.ok(duckDbService.getDistinctValues(id, column, filterMap, limit))
    }

    @GetMapping("/{id}/null-columns")
    fun getNullColumns(@PathVariable id: Long): ResponseEntity<List<String>> {
        duckDbService.getDataset(id) ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(duckDbService.getNullColumns(id))
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

    @PostMapping("/upload")
    fun upload(
        @RequestParam("file") file: MultipartFile,
        @RequestParam("name") name: String
    ): ResponseEntity<Map<String, Any>> {
        val datasetId = duckDbService.getNextId()
        val tempDir = File(properties.cacheDir, "tmp/upload_$datasetId").also { it.mkdirs() }

        try {
            val originalName = file.originalFilename ?: "upload"
            val localFile = File(tempDir, originalName)
            file.transferTo(localFile)

            val isParquet = originalName.lowercase().let { it.endsWith(".parquet") || it.endsWith(".snappy.parquet") }
            val isCsv = originalName.lowercase().let { it.endsWith(".csv") || it.endsWith(".csv.gz") }

            if (isParquet) {
                duckDbService.importParquetFile(localFile.absolutePath, datasetId)
            } else if (isCsv) {
                duckDbService.importCsvFile(localFile.absolutePath, datasetId)
            } else {
                return ResponseEntity.badRequest().body(mapOf("error" to "Unsupported file type. Use .parquet or .csv" as Any))
            }

            val schema = duckDbService.getSchema(datasetId)
            val rowCount = duckDbService.getRowCount(datasetId)
            val sourceType = if (isParquet) SourceType.LOCAL_PARQUET else SourceType.LOCAL_CSV

            duckDbService.registerDataset(
                id = datasetId,
                name = name,
                sourceUri = "upload://$originalName",
                sourceType = sourceType,
                entityPath = null,
                runTimestamp = null,
                rowCount = rowCount,
                sizeBytes = file.size,
                schema = schema
            )

            duckDbService.checkpoint()
            log.info("Upload complete: dataset $datasetId ($name), $rowCount rows, ${file.size} bytes")

            return ResponseEntity.ok(mapOf(
                "datasetId" to datasetId as Any,
                "name" to name as Any,
                "rowCount" to rowCount as Any
            ))
        } finally {
            tempDir.deleteRecursively()
        }
    }

    @DeleteMapping("/{id}")
    fun deleteDataset(@PathVariable id: Long): ResponseEntity<Void> {
        duckDbService.getDataset(id) ?: return ResponseEntity.notFound().build()
        duckDbService.deleteDataset(id)
        return ResponseEntity.noContent().build()
    }
}
