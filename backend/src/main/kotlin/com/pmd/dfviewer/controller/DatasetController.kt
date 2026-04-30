package com.pmd.dfviewer.controller

import com.pmd.dfviewer.config.DfViewerProperties
import com.pmd.dfviewer.model.ColumnJoinConfig
import com.pmd.dfviewer.model.*
import com.pmd.dfviewer.service.DuckDbService
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.multipart.MultipartFile
import java.io.File
import java.util.zip.ZipInputStream

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

    @GetMapping("/{id:\\d+}")
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

    @GetMapping("/{id}/column-joins")
    fun getColumnJoins(@PathVariable id: Long): ResponseEntity<List<ColumnJoinConfig>> {
        return ResponseEntity.ok(duckDbService.getColumnJoins(id))
    }

    @PutMapping("/{id}/column-joins")
    fun setColumnJoins(@PathVariable id: Long, @RequestBody joins: List<ColumnJoinConfig>): ResponseEntity<Void> {
        duckDbService.setColumnJoins(id, joins)
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
        @RequestParam("files") files: List<MultipartFile>,
        @RequestParam("name") name: String
    ): ResponseEntity<Map<String, Any>> {
        if (files.isEmpty()) {
            return ResponseEntity.badRequest().body(mapOf("error" to "No files provided" as Any))
        }
        val datasetId = duckDbService.getNextId()
        val tempDir = File(properties.cacheDir, "tmp/upload_$datasetId").also { it.mkdirs() }

        try {
            val totalBytes = files.sumOf { it.size }
            val collected = mutableListOf<File>()

            files.forEachIndexed { idx, mf ->
                val origName = mf.originalFilename ?: "upload_$idx"
                val srcDir = File(tempDir, "src_$idx").also { it.mkdirs() }
                val saved = File(srcDir, sanitizeFilename(origName))
                mf.transferTo(saved)
                if (saved.name.lowercase().endsWith(".zip")) {
                    extractZipInto(saved, srcDir, collected)
                    saved.delete()
                } else if (isSupportedDataFile(saved.name)) {
                    collected += saved
                }
            }

            if (collected.isEmpty()) {
                return ResponseEntity.badRequest().body(mapOf(
                    "error" to "No supported files found. Accepted: .parquet, .csv, .csv.gz, .zip" as Any
                ))
            }

            val parquet = collected.filter {
                val n = it.name.lowercase(); n.endsWith(".parquet") || n.endsWith(".snappy.parquet")
            }
            val csv = collected.filter {
                val n = it.name.lowercase(); n.endsWith(".csv") || n.endsWith(".csv.gz")
            }

            if (parquet.isNotEmpty() && csv.isNotEmpty()) {
                return ResponseEntity.badRequest().body(mapOf(
                    "error" to "Mixed parquet and csv files in one upload are not supported" as Any
                ))
            }

            val sourceType: SourceType
            val sourceUri: String
            if (parquet.isNotEmpty()) {
                duckDbService.importParquetFiles(parquet.map { it.absolutePath }, datasetId)
                sourceType = SourceType.LOCAL_PARQUET
                sourceUri = if (parquet.size == 1) "upload://${parquet[0].name}"
                            else "upload://${parquet.size} parquet files"
            } else {
                duckDbService.importCsvFile(csv[0].absolutePath, datasetId)
                csv.drop(1).forEach { duckDbService.appendCsvFile(it.absolutePath, datasetId) }
                sourceType = SourceType.LOCAL_CSV
                sourceUri = if (csv.size == 1) "upload://${csv[0].name}"
                            else "upload://${csv.size} csv files"
            }

            val schema = duckDbService.getSchema(datasetId)
            val rowCount = duckDbService.getRowCount(datasetId)

            duckDbService.registerDataset(
                id = datasetId,
                name = name,
                sourceUri = sourceUri,
                sourceType = sourceType,
                entityPath = null,
                runTimestamp = null,
                rowCount = rowCount,
                sizeBytes = totalBytes,
                schema = schema
            )

            duckDbService.checkpoint()
            log.info("Upload complete: dataset $datasetId ($name), $rowCount rows, $totalBytes bytes, ${collected.size} input file(s)")

            return ResponseEntity.ok(mapOf(
                "datasetId" to datasetId as Any,
                "name" to name as Any,
                "rowCount" to rowCount as Any,
                "fileCount" to collected.size as Any
            ))
        } finally {
            tempDir.deleteRecursively()
        }
    }

    private fun isSupportedDataFile(name: String): Boolean {
        val n = name.lowercase()
        return n.endsWith(".parquet") || n.endsWith(".snappy.parquet") ||
                n.endsWith(".csv") || n.endsWith(".csv.gz")
    }

    private fun sanitizeFilename(name: String): String =
        name.substringAfterLast('/').substringAfterLast('\\').replace("..", "_")

    private fun extractZipInto(zip: File, destDir: File, out: MutableList<File>) {
        ZipInputStream(zip.inputStream().buffered()).use { zis ->
            var entry = zis.nextEntry
            while (entry != null) {
                if (!entry.isDirectory && isSupportedDataFile(entry.name)) {
                    val safeName = sanitizeFilename(entry.name)
                    var target = File(destDir, safeName)
                    if (target.exists()) target = File(destDir, "${System.nanoTime()}_$safeName")
                    target.outputStream().buffered().use { os -> zis.copyTo(os) }
                    out += target
                }
                entry = zis.nextEntry
            }
        }
    }

    @PutMapping("/{id:\\d+}/name")
    fun renameDataset(@PathVariable id: Long, @RequestBody body: Map<String, String>): ResponseEntity<Void> {
        val name = body["name"]?.trim()
        if (name.isNullOrEmpty()) return ResponseEntity.badRequest().build()
        duckDbService.getDataset(id) ?: return ResponseEntity.notFound().build()
        duckDbService.updateDatasetName(id, name)
        duckDbService.checkpoint()
        return ResponseEntity.noContent().build()
    }

    @DeleteMapping("/{id:\\d+}")
    fun deleteDataset(@PathVariable id: Long): ResponseEntity<Void> {
        duckDbService.getDataset(id) ?: return ResponseEntity.notFound().build()
        duckDbService.deleteDataset(id)
        return ResponseEntity.noContent().build()
    }
}
