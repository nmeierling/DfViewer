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
        connection = DriverManager.getConnection("jdbc:duckdb:${properties.duckdbPath}")
        createRegistryTable()
        log.info("DuckDB initialized at ${properties.duckdbPath}")
    }

    @PreDestroy
    fun close() {
        if (::connection.isInitialized && !connection.isClosed) {
            connection.close()
        }
    }

    private fun createRegistryTable() {
        connection.createStatement().use { stmt ->
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS dataset_registry (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    source_uri VARCHAR NOT NULL,
                    source_type VARCHAR NOT NULL,
                    entity_path VARCHAR,
                    run_timestamp VARCHAR,
                    imported_at TIMESTAMP NOT NULL,
                    row_count BIGINT NOT NULL DEFAULT 0,
                    schema_json VARCHAR NOT NULL DEFAULT '[]'
                )
            """)
            stmt.execute("""
                CREATE SEQUENCE IF NOT EXISTS dataset_id_seq START 1
            """)
        }
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
    fun registerDataset(
        id: Long,
        name: String,
        sourceUri: String,
        sourceType: SourceType,
        entityPath: String?,
        runTimestamp: String?,
        rowCount: Long,
        schema: List<ColumnInfo>
    ) {
        val schemaJson = objectMapper.writeValueAsString(schema)
        connection.prepareStatement("""
            INSERT INTO dataset_registry (id, name, source_uri, source_type, entity_path, run_timestamp, imported_at, row_count, schema_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """).use { stmt ->
            stmt.setLong(1, id)
            stmt.setString(2, name)
            stmt.setString(3, sourceUri)
            stmt.setString(4, sourceType.name)
            stmt.setString(5, entityPath)
            stmt.setString(6, runTimestamp)
            stmt.setTimestamp(7, java.sql.Timestamp.from(Instant.now()))
            stmt.setLong(8, rowCount)
            stmt.setString(9, schemaJson)
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
                schemaJson = getString("schema_json")
            ))
        }
        return datasets
    }
}
