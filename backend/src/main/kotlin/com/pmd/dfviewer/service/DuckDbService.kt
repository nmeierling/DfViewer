package com.pmd.dfviewer.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.pmd.dfviewer.config.DfViewerProperties
import com.pmd.dfviewer.model.ColumnInfo
import com.pmd.dfviewer.model.ColumnJoinConfig
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
        executeSQL("CREATE SEQUENCE IF NOT EXISTS dataset_id_seq START 1")

        executeSQL("""
            CREATE TABLE IF NOT EXISTS column_settings (
                dataset_id INTEGER NOT NULL,
                hidden_columns VARCHAR NOT NULL DEFAULT '[]',
                column_widths VARCHAR NOT NULL DEFAULT '{}',
                column_order VARCHAR NOT NULL DEFAULT '[]',
                column_joins VARCHAR NOT NULL DEFAULT '[]',
                PRIMARY KEY (dataset_id)
            )
        """)
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
    fun getHiddenColumns(datasetId: Long): List<String> {
        connection.prepareStatement("SELECT hidden_columns FROM column_settings WHERE dataset_id = ?").use { stmt ->
            stmt.setLong(1, datasetId)
            val rs = stmt.executeQuery()
            if (rs.next()) {
                return objectMapper.readValue(rs.getString("hidden_columns"), List::class.java).map { it.toString() }
            }
            return emptyList()
        }
    }

    private fun ensureColumnSettingsRow(datasetId: Long) {
        connection.prepareStatement(
            "INSERT OR IGNORE INTO column_settings (dataset_id, hidden_columns, column_widths, column_order, column_joins) VALUES (?, '[]', '{}', '[]', '[]')"
        ).use { stmt ->
            stmt.setLong(1, datasetId)
            stmt.executeUpdate()
        }
    }

    @Synchronized
    fun setHiddenColumns(datasetId: Long, hiddenColumns: List<String>) {
        ensureColumnSettingsRow(datasetId)
        val json = objectMapper.writeValueAsString(hiddenColumns)
        connection.prepareStatement("UPDATE column_settings SET hidden_columns = ? WHERE dataset_id = ?").use { stmt ->
            stmt.setString(1, json)
            stmt.setLong(2, datasetId)
            stmt.executeUpdate()
        }
    }

    @Synchronized
    fun getColumnOrder(datasetId: Long): List<String> {
        connection.prepareStatement("SELECT column_order FROM column_settings WHERE dataset_id = ?").use { stmt ->
            stmt.setLong(1, datasetId)
            val rs = stmt.executeQuery()
            if (rs.next()) {
                return objectMapper.readValue(rs.getString("column_order"), List::class.java).map { it.toString() }
            }
            return emptyList()
        }
    }

    @Synchronized
    fun setColumnOrder(datasetId: Long, columnOrder: List<String>) {
        ensureColumnSettingsRow(datasetId)
        val json = objectMapper.writeValueAsString(columnOrder)
        connection.prepareStatement("UPDATE column_settings SET column_order = ? WHERE dataset_id = ?").use { stmt ->
            stmt.setString(1, json)
            stmt.setLong(2, datasetId)
            stmt.executeUpdate()
        }
    }

    @Synchronized
    fun getColumnWidths(datasetId: Long): Map<String, Int> {
        connection.prepareStatement("SELECT column_widths FROM column_settings WHERE dataset_id = ?").use { stmt ->
            stmt.setLong(1, datasetId)
            val rs = stmt.executeQuery()
            if (rs.next()) {
                val json = rs.getString("column_widths")
                return objectMapper.readValue(json, Map::class.java).map { (k, v) -> k.toString() to (v as Number).toInt() }.toMap()
            }
            return emptyMap()
        }
    }

    @Synchronized
    fun setColumnWidths(datasetId: Long, widths: Map<String, Int>) {
        ensureColumnSettingsRow(datasetId)
        val json = objectMapper.writeValueAsString(widths)
        connection.prepareStatement("UPDATE column_settings SET column_widths = ? WHERE dataset_id = ?").use { stmt ->
            stmt.setString(1, json)
            stmt.setLong(2, datasetId)
            stmt.executeUpdate()
        }
    }

    @Synchronized
    fun getColumnJoins(datasetId: Long): List<ColumnJoinConfig> {
        try {
            connection.prepareStatement("SELECT column_joins FROM column_settings WHERE dataset_id = ?").use { stmt ->
                stmt.setLong(1, datasetId)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    val json = rs.getString("column_joins")
                    if (json.isNullOrBlank() || json == "[]") return emptyList()
                    return objectMapper.readValue(json,
                        objectMapper.typeFactory.constructCollectionType(List::class.java, ColumnJoinConfig::class.java))
                }
                return emptyList()
            }
        } catch (e: Exception) {
            log.warn("Failed to read column_joins for dataset $datasetId: ${e.message}")
            return emptyList()
        }
    }

    @Synchronized
    fun setColumnJoins(datasetId: Long, joins: List<ColumnJoinConfig>) {
        ensureColumnSettingsRow(datasetId)
        val json = objectMapper.writeValueAsString(joins)
        connection.prepareStatement("UPDATE column_settings SET column_joins = ? WHERE dataset_id = ?").use { stmt ->
            stmt.setString(1, json)
            stmt.setLong(2, datasetId)
            stmt.executeUpdate()
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
    private fun buildWhereClause(filters: Map<String, String>?, tableAlias: String? = null): String {
        val prefix = if (tableAlias != null) "$tableAlias." else ""
        val whereClauses = mutableListOf<String>()
        filters?.forEach { (col, value) ->
            val safeCol = col.replace("\"", "\"\"")
            when {
                value.equals("\$null", ignoreCase = true) ->
                    whereClauses.add("$prefix\"$safeCol\" IS NULL")
                value.equals("\$notnull", ignoreCase = true) ->
                    whereClauses.add("$prefix\"$safeCol\" IS NOT NULL")
                value.equals("\$empty", ignoreCase = true) ->
                    whereClauses.add("($prefix\"$safeCol\" IS NULL OR CAST($prefix\"$safeCol\" AS VARCHAR) = '')")
                value.equals("\$notempty", ignoreCase = true) ->
                    whereClauses.add("($prefix\"$safeCol\" IS NOT NULL AND CAST($prefix\"$safeCol\" AS VARCHAR) != '')")
                else -> {
                    val safeVal = value.replace("'", "''")
                    whereClauses.add("CAST($prefix\"$safeCol\" AS VARCHAR) ILIKE '%$safeVal%'")
                }
            }
        }
        return if (whereClauses.isNotEmpty()) "WHERE ${whereClauses.joinToString(" AND ")}" else ""
    }

    private fun buildDisplayExpr(template: String, alias: String): String {
        // Parse "{field1} ({field2})" -> CONCAT(alias."field1", ' (', alias."field2", ')')
        val parts = mutableListOf<String>()
        val regex = Regex("\\{([^}]+)\\}")
        var lastEnd = 0
        for (match in regex.findAll(template)) {
            if (match.range.first > lastEnd) {
                val literal = template.substring(lastEnd, match.range.first).replace("'", "''")
                parts.add("'$literal'")
            }
            val fieldName = match.groupValues[1].replace("\"", "\"\"")
            parts.add("CAST($alias.\"$fieldName\" AS VARCHAR)")
            lastEnd = match.range.last + 1
        }
        if (lastEnd < template.length) {
            val literal = template.substring(lastEnd).replace("'", "''")
            parts.add("'$literal'")
        }
        return if (parts.size == 1) parts[0] else "CONCAT(${parts.joinToString(", ")})"
    }

    @Synchronized
    fun getDistinctValues(datasetId: Long, column: String, filters: Map<String, String>?, limit: Int = 500): List<Map<String, Any?>> {
        val tableName = "ds_$datasetId"
        val safeCol = column.replace("\"", "\"\"")
        val whereStr = buildWhereClause(filters)
        val sql = """
            SELECT "\"$safeCol\"" AS value, COUNT(*) AS count
            FROM $tableName $whereStr
            GROUP BY "\"$safeCol\""
            ORDER BY count DESC
            LIMIT $limit
        """.trimIndent()
            .replace("\"\"", "\"") // fix double-escaping from template

        // Build SQL manually to avoid escaping issues
        val actualSql = "SELECT \"$safeCol\" AS value, COUNT(*) AS count FROM $tableName $whereStr GROUP BY \"$safeCol\" ORDER BY count DESC LIMIT $limit"

        val results = mutableListOf<Map<String, Any?>>()
        connection.createStatement().use { stmt ->
            val rs = stmt.executeQuery(actualSql)
            while (rs.next()) {
                results.add(mapOf(
                    "value" to rs.getObject("value"),
                    "count" to rs.getLong("count")
                ))
            }
        }
        return results
    }

    @Synchronized
    fun queryData(datasetId: Long, page: Int, pageSize: Int, sortColumn: String?, sortDirection: String?, filters: Map<String, String>?): DataPage {
        val tableName = "ds_$datasetId"
        val joins = getColumnJoins(datasetId)
        val hasJoins = joins.isNotEmpty()
        val whereStr = if (hasJoins) buildWhereClause(filters, "t") else buildWhereClause(filters)
        if (hasJoins) log.info("queryData ds_$datasetId: ${joins.size} joins configured")

        val orderStr = if (sortColumn != null) {
            val safeCol = sortColumn.replace("\"", "\"\"")
            val dir = if (sortDirection?.uppercase() == "DESC") "DESC" else "ASC"
            "ORDER BY \"$safeCol\" $dir"
        } else ""

        val offset = page * pageSize

        val countSql: String
        val dataSql: String

        if (hasJoins) {
            val joinClauses = StringBuilder()
            val selectExprs = mutableListOf<String>()
            selectExprs.add("t.*")

            joins.forEachIndexed { idx, join ->
                val alias = "j$idx"
                val joinTable = "ds_${join.joinDatasetId}"
                val srcCol = join.sourceColumn.replace("\"", "\"\"")
                val joinCol = join.joinColumn.replace("\"", "\"\"")
                joinClauses.append(" LEFT JOIN $joinTable $alias ON t.\"$srcCol\" = $alias.\"$joinCol\"")

                val displayExpr = buildDisplayExpr(join.displayTemplate, alias)
                val displayColName = "${join.sourceColumn}_display"
                selectExprs.add("$displayExpr AS \"${displayColName.replace("\"", "\"\"")}\"")
            }

            val fromClause = "$tableName t$joinClauses"
            val selectStr = selectExprs.joinToString(", ")
            countSql = "SELECT COUNT(*) FROM $fromClause $whereStr"
            dataSql = "SELECT $selectStr FROM $fromClause $whereStr $orderStr LIMIT $pageSize OFFSET $offset"
            log.info("queryData SQL: $dataSql")
        } else {
            countSql = "SELECT COUNT(*) FROM $tableName $whereStr"
            dataSql = "SELECT * FROM $tableName $whereStr $orderStr LIMIT $pageSize OFFSET $offset"
        }

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
