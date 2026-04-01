package com.pmd.dfviewer.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pmd.dfviewer.config.DfViewerProperties
import com.pmd.dfviewer.model.ColumnInfo
import com.pmd.dfviewer.model.DataPage
import com.pmd.dfviewer.model.Dataset
import com.pmd.dfviewer.model.SourceType
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.time.Instant

@Service
class DuckDbService(
    private val properties: DfViewerProperties,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private lateinit var connection: Connection

    @PostConstruct
    fun init() {
        File(properties.cacheDir).mkdirs()
        Class.forName("org.duckdb.DuckDBDriver")

        val dbPath = properties.duckdbPath
        val walFile = File("$dbPath.wal")

        try {
            connection = DriverManager.getConnection("jdbc:duckdb:$dbPath")
            // Verify connection works
            connection.createStatement().use { it.executeQuery("SELECT 1") }
        } catch (e: Exception) {
            log.warn("Failed to open DuckDB, attempting recovery: ${e.message}")
            // Close broken connection if partially opened
            try { connection.close() } catch (_: Exception) {}
            // Delete stale WAL file if it exists
            if (walFile.exists()) {
                log.warn("Deleting stale WAL file: ${walFile.absolutePath}")
                walFile.delete()
            }
            try {
                connection = DriverManager.getConnection("jdbc:duckdb:$dbPath")
                connection.createStatement().use { it.executeQuery("SELECT 1") }
            } catch (e2: Exception) {
                // DB file is corrupted — rename and start fresh
                val dbFile = File(dbPath)
                val backupFile = File("$dbPath.corrupted.${System.currentTimeMillis()}")
                log.error("DuckDB file is corrupted, backing up to ${backupFile.name} and starting fresh", e2)
                dbFile.renameTo(backupFile)
                connection = DriverManager.getConnection("jdbc:duckdb:$dbPath")
            }
        }

        createRegistryTable()
        log.info("DuckDB initialized at $dbPath")
    }

    @PreDestroy
    fun close() {
        try {
            if (::connection.isInitialized && !connection.isClosed) {
                connection.createStatement().use { it.execute("CHECKPOINT") }
                connection.close()
                log.info("DuckDB closed cleanly")
            }
        } catch (e: Exception) {
            log.warn("Error during DuckDB shutdown: ${e.message}")
        }
    }

    @Synchronized
    fun checkpoint() {
        connection.createStatement().use { stmt ->
            stmt.execute("CHECKPOINT")
        }
        log.info("DuckDB checkpoint complete")
    }

    private fun createRegistryTable() {
        executeSQL("""
            CREATE TABLE IF NOT EXISTS dataset_registry (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                source_uri VARCHAR NOT NULL,
                source_type VARCHAR NOT NULL,
                entity_path VARCHAR,
                run_timestamp VARCHAR,
                imported_at TIMESTAMP NOT NULL,
                row_count BIGINT NOT NULL DEFAULT 0,
                size_bytes BIGINT NOT NULL DEFAULT 0,
                schema_json VARCHAR NOT NULL DEFAULT '[]'
            )
        """)
        // Migration: add size_bytes column if missing (for older DBs)
        try { executeSQL("ALTER TABLE dataset_registry ADD COLUMN IF NOT EXISTS size_bytes BIGINT NOT NULL DEFAULT 0") } catch (_: Exception) {}
        executeSQL("CREATE SEQUENCE IF NOT EXISTS dataset_id_seq START 1")
    }

    private fun executeSQL(sql: String) {
        connection.createStatement().use { it.execute(sql) }
    }

    @Synchronized
    fun executeSQLSync(sql: String) {
        connection.createStatement().use { it.execute(sql) }
    }

    @Synchronized
    fun countTable(tableName: String): Long {
        connection.createStatement().use { stmt ->
            val rs = stmt.executeQuery("SELECT COUNT(*) FROM $tableName")
            rs.next()
            return rs.getLong(1)
        }
    }

    @Synchronized
    fun queryView(viewName: String, page: Int, pageSize: Int): DataPage {
        val offset = page * pageSize
        val totalRows = countTable(viewName)

        val rows = mutableListOf<Map<String, Any?>>()
        connection.createStatement().use { stmt ->
            val rs = stmt.executeQuery("SELECT * FROM $viewName LIMIT $pageSize OFFSET $offset")
            val meta = rs.metaData
            val colCount = meta.columnCount
            while (rs.next()) {
                val row = linkedMapOf<String, Any?>()
                for (i in 1..colCount) {
                    row[meta.getColumnName(i)] = rs.getObject(i)
                }
                rows.add(row)
            }
        }

        val totalPages = if (totalRows == 0L) 1 else ((totalRows + pageSize - 1) / pageSize).toInt()
        return DataPage(data = rows, totalRows = totalRows, page = page, pageSize = pageSize, totalPages = totalPages)
    }

    @Synchronized
    fun getNextId(): Long {
        connection.createStatement().use { stmt ->
            val rs = stmt.executeQuery("SELECT nextval('dataset_id_seq')")
            rs.next()
            return rs.getLong(1)
        }
    }

    @Synchronized
    fun importParquetFile(filePath: String, datasetId: Long) {
        val tableName = "ds_$datasetId"
        connection.createStatement().use { stmt ->
            stmt.execute("CREATE TABLE $tableName AS SELECT * FROM read_parquet('$filePath')")
        }
    }

    @Synchronized
    fun importParquetGlob(globPattern: String, datasetId: Long) {
        val tableName = "ds_$datasetId"
        connection.createStatement().use { stmt ->
            stmt.execute("CREATE TABLE $tableName AS SELECT * FROM read_parquet('$globPattern', union_by_name=true)")
        }
    }

    @Synchronized
    fun importCsvFile(filePath: String, datasetId: Long) {
        val tableName = "ds_$datasetId"
        connection.createStatement().use { stmt ->
            stmt.execute("CREATE TABLE $tableName AS SELECT * FROM read_csv('$filePath', auto_detect=true)")
        }
    }

    @Synchronized
    fun appendParquetFile(filePath: String, datasetId: Long) {
        val tableName = "ds_$datasetId"
        connection.createStatement().use { stmt ->
            stmt.execute("INSERT INTO $tableName SELECT * FROM read_parquet('$filePath')")
        }
    }

    @Synchronized
    fun appendCsvFile(filePath: String, datasetId: Long) {
        val tableName = "ds_$datasetId"
        connection.createStatement().use { stmt ->
            stmt.execute("INSERT INTO $tableName SELECT * FROM read_csv('$filePath', auto_detect=true)")
        }
    }

    @Synchronized
    fun getRowCount(datasetId: Long): Long {
        val tableName = "ds_$datasetId"
        connection.createStatement().use { stmt ->
            val rs = stmt.executeQuery("SELECT COUNT(*) FROM $tableName")
            rs.next()
            return rs.getLong(1)
        }
    }

    @Synchronized
    fun getSchema(datasetId: Long): List<ColumnInfo> {
        val tableName = "ds_$datasetId"
        connection.createStatement().use { stmt ->
            val rs = stmt.executeQuery("DESCRIBE $tableName")
            val columns = mutableListOf<ColumnInfo>()
            while (rs.next()) {
                columns.add(ColumnInfo(
                    name = rs.getString("column_name"),
                    type = rs.getString("column_type")
                ))
            }
            return columns
        }
    }

    @Synchronized
    fun getNullColumns(datasetId: Long): List<String> {
        val tableName = "ds_$datasetId"
        val schema = getSchema(datasetId)
        if (schema.isEmpty()) return emptyList()

        val countExprs = schema.joinToString(", ") { col ->
            val safeCol = col.name.replace("\"", "\"\"")
            "COUNT(\"$safeCol\") AS \"cnt_${safeCol}\""
        }
        connection.createStatement().use { stmt ->
            val rs = stmt.executeQuery("SELECT $countExprs FROM $tableName")
            rs.next()
            return schema.filter { col ->
                val safeCol = col.name.replace("\"", "\"\"")
                rs.getLong("cnt_$safeCol") == 0L
            }.map { it.name }
        }
    }

    @Synchronized
    fun registerDataset(
        id: Long,
        name: String,
        sourceUri: String,
        sourceType: SourceType,
        entityPath: String?,
        runTimestamp: String?,
        rowCount: Long,
        sizeBytes: Long,
        schema: List<ColumnInfo>
    ) {
        val schemaJson = objectMapper.writeValueAsString(schema)
        connection.prepareStatement("""
            INSERT INTO dataset_registry (id, name, source_uri, source_type, entity_path, run_timestamp, imported_at, row_count, size_bytes, schema_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """).use { stmt ->
            stmt.setLong(1, id)
            stmt.setString(2, name)
            stmt.setString(3, sourceUri)
            stmt.setString(4, sourceType.name)
            stmt.setString(5, entityPath)
            stmt.setString(6, runTimestamp)
            stmt.setTimestamp(7, java.sql.Timestamp.from(Instant.now()))
            stmt.setLong(8, rowCount)
            stmt.setLong(9, sizeBytes)
            stmt.setString(10, schemaJson)
            stmt.executeUpdate()
        }
    }

    @Synchronized
    fun listDatasets(): List<Dataset> {
        connection.createStatement().use { stmt ->
            val rs = stmt.executeQuery("SELECT * FROM dataset_registry ORDER BY imported_at DESC")
            return rs.toDatasets()
        }
    }

    @Synchronized
    fun getDataset(id: Long): Dataset? {
        connection.prepareStatement("SELECT * FROM dataset_registry WHERE id = ?").use { stmt ->
            stmt.setLong(1, id)
            val rs = stmt.executeQuery()
            val list = rs.toDatasets()
            return list.firstOrNull()
        }
    }

    @Synchronized
    fun deleteDataset(id: Long) {
        val tableName = "ds_$id"
        connection.createStatement().use { stmt ->
            stmt.execute("DROP TABLE IF EXISTS $tableName")
            stmt.execute("DELETE FROM dataset_registry WHERE id = $id")
        }
    }

    @Synchronized
    fun queryData(datasetId: Long, page: Int, pageSize: Int, sortColumn: String?, sortDirection: String?, filters: Map<String, String>?): DataPage {
        val tableName = "ds_$datasetId"
        val whereClauses = mutableListOf<String>()
        filters?.forEach { (col, value) ->
            val safeCol = col.replace("\"", "\"\"")
            val safeVal = value.replace("'", "''")
            whereClauses.add("CAST(\"$safeCol\" AS VARCHAR) ILIKE '%$safeVal%'")
        }
        val whereStr = if (whereClauses.isNotEmpty()) "WHERE ${whereClauses.joinToString(" AND ")}" else ""

        val orderStr = if (sortColumn != null) {
            val safeCol = sortColumn.replace("\"", "\"\"")
            val dir = if (sortDirection?.uppercase() == "DESC") "DESC" else "ASC"
            "ORDER BY \"$safeCol\" $dir"
        } else ""

        val offset = page * pageSize

        val countSql = "SELECT COUNT(*) FROM $tableName $whereStr"
        val dataSql = "SELECT * FROM $tableName $whereStr $orderStr LIMIT $pageSize OFFSET $offset"

        val totalRows: Long
        connection.createStatement().use { stmt ->
            val rs = stmt.executeQuery(countSql)
            rs.next()
            totalRows = rs.getLong(1)
        }

        val rows = mutableListOf<Map<String, Any?>>()
        connection.createStatement().use { stmt ->
            val rs = stmt.executeQuery(dataSql)
            val meta = rs.metaData
            val colCount = meta.columnCount
            while (rs.next()) {
                val row = linkedMapOf<String, Any?>()
                for (i in 1..colCount) {
                    row[meta.getColumnName(i)] = rs.getObject(i)
                }
                rows.add(row)
            }
        }

        val totalPages = if (totalRows == 0L) 1 else ((totalRows + pageSize - 1) / pageSize).toInt()
        return DataPage(
            data = rows,
            totalRows = totalRows,
            page = page,
            pageSize = pageSize,
            totalPages = totalPages
        )
    }

    private fun ResultSet.toDatasets(): List<Dataset> {
        val datasets = mutableListOf<Dataset>()
        while (next()) {
            datasets.add(Dataset(
                id = getLong("id"),
                name = getString("name"),
                sourceUri = getString("source_uri"),
                sourceType = SourceType.valueOf(getString("source_type")),
                entityPath = getString("entity_path"),
                runTimestamp = getString("run_timestamp"),
                importedAt = getTimestamp("imported_at")?.toInstant()
                    ?: Instant.now(),
                rowCount = getLong("row_count"),
                sizeBytes = getLong("size_bytes"),
                schemaJson = getString("schema_json")
            ))
        }
        return datasets
    }
}
