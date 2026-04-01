package com.pmd.dfviewer.service

import com.pmd.dfviewer.model.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

data class ColumnChangeSummary(
    val column: String,
    val changedCount: Long
)

@Service
class ComparisonService(
    private val duckDbService: DuckDbService
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun compare(request: CompareRequest): ComparisonSummary {
        val leftId = request.leftDatasetId
        val rightId = request.rightDatasetId
        val keyColumns = request.keyColumns
        val leftTable = "ds_$leftId"
        val rightTable = "ds_$rightId"

        val leftDs = duckDbService.getDataset(leftId)
            ?: throw IllegalArgumentException("Left dataset $leftId not found")
        val rightDs = duckDbService.getDataset(rightId)
            ?: throw IllegalArgumentException("Right dataset $rightId not found")

        val keyJoin = keyColumns.joinToString(" AND ") { "l.${q(it)} = r.${q(it)}" }
        val keyIsNull = keyColumns.joinToString(" AND ") { "r.${q(it)} IS NULL" }
        val keyIsNullLeft = keyColumns.joinToString(" AND ") { "l.${q(it)} IS NULL" }

        val ignoreColumns = request.ignoreColumns.toSet()

        // Get non-key columns present in both datasets, minus ignored
        val leftSchema = duckDbService.getSchema(leftId).map { it.name }
        val rightSchema = duckDbService.getSchema(rightId).map { it.name }
        val commonColumns = leftSchema.intersect(rightSchema.toSet())
        val valueColumns = commonColumns.minus(keyColumns.toSet()).minus(ignoreColumns).toList()

        // Detect null columns in both datasets — exclude from comparison views
        val leftNulls = duckDbService.getNullColumns(leftId).toSet()
        val rightNulls = duckDbService.getNullColumns(rightId).toSet()
        val bothNull = leftNulls.intersect(rightNulls)
        val visibleValueColumns = valueColumns.filter { it !in bothNull }

        val compId = "cmp_${leftId}_${rightId}"

        // Added: exclude null + ignored columns
        val addedVisibleCols = rightSchema.filter { it !in rightNulls && it !in ignoreColumns }
        duckDbService.executeSQLSync("""
            CREATE OR REPLACE VIEW ${compId}_added AS
            SELECT ${addedVisibleCols.joinToString(", ") { "r.${q(it)}" }}
            FROM $rightTable r
            LEFT JOIN $leftTable l ON $keyJoin
            WHERE $keyIsNullLeft
        """)

        // Removed: exclude null + ignored columns
        val removedVisibleCols = leftSchema.filter { it !in leftNulls && it !in ignoreColumns }
        duckDbService.executeSQLSync("""
            CREATE OR REPLACE VIEW ${compId}_removed AS
            SELECT ${removedVisibleCols.joinToString(", ") { "l.${q(it)}" }}
            FROM $leftTable l
            LEFT JOIN $rightTable r ON $keyJoin
            WHERE $keyIsNull
        """)

        // Changed: rows with at least one visible value column different
        if (visibleValueColumns.isNotEmpty()) {
            val changeCondition = visibleValueColumns.joinToString(" OR ") { col ->
                "(l.${q(col)} IS DISTINCT FROM r.${q(col)})"
            }
            duckDbService.executeSQLSync("""
                CREATE OR REPLACE VIEW ${compId}_changed AS
                SELECT ${keyColumns.joinToString(", ") { "l.${q(it)}" }},
                    ${visibleValueColumns.joinToString(", ") { col ->
                        "l.${q(col)} AS ${q("left_$col")}, r.${q(col)} AS ${q("right_$col")}"
                    }}
                FROM $leftTable l
                INNER JOIN $rightTable r ON $keyJoin
                WHERE $changeCondition
            """)

            // Per-column change views
            for (col in visibleValueColumns) {
                duckDbService.executeSQLSync("""
                    CREATE OR REPLACE VIEW ${compId}_col_${sanitize(col)} AS
                    SELECT ${keyColumns.joinToString(", ") { "l.${q(it)}" }},
                        l.${q(col)} AS ${q("left_$col")},
                        r.${q(col)} AS ${q("right_$col")}
                    FROM $leftTable l
                    INNER JOIN $rightTable r ON $keyJoin
                    WHERE l.${q(col)} IS DISTINCT FROM r.${q(col)}
                """)
            }
        } else {
            duckDbService.executeSQLSync("""
                CREATE OR REPLACE VIEW ${compId}_changed AS
                SELECT ${keyColumns.joinToString(", ") { "l.${q(it)}" }}
                FROM $leftTable l INNER JOIN $rightTable r ON $keyJoin WHERE false
            """)
        }

        // Counts
        val addedCount = duckDbService.countTable("${compId}_added")
        val removedCount = duckDbService.countTable("${compId}_removed")
        val changedCount = duckDbService.countTable("${compId}_changed")
        val totalLeft = duckDbService.getRowCount(leftId)
        val totalRight = duckDbService.getRowCount(rightId)
        val unchangedCount = totalLeft - removedCount - changedCount

        log.info("Comparison $compId: added=$addedCount, removed=$removedCount, changed=$changedCount, unchanged=$unchangedCount")

        return ComparisonSummary(
            leftDatasetId = leftId,
            rightDatasetId = rightId,
            leftName = leftDs.name,
            rightName = rightDs.name,
            keyColumns = keyColumns,
            totalLeft = totalLeft,
            totalRight = totalRight,
            addedCount = addedCount,
            removedCount = removedCount,
            changedCount = changedCount,
            unchangedCount = unchangedCount
        )
    }

    fun getAdded(leftId: Long, rightId: Long, page: Int, size: Int): DataPage {
        return duckDbService.queryView("cmp_${leftId}_${rightId}_added", page, size)
    }

    fun getRemoved(leftId: Long, rightId: Long, page: Int, size: Int): DataPage {
        return duckDbService.queryView("cmp_${leftId}_${rightId}_removed", page, size)
    }

    fun getChanged(leftId: Long, rightId: Long, page: Int, size: Int): DataPage {
        return duckDbService.queryView("cmp_${leftId}_${rightId}_changed", page, size)
    }

    fun getColumnChanges(leftId: Long, rightId: Long): List<ColumnChangeSummary> {
        val compId = "cmp_${leftId}_${rightId}"
        val leftSchema = duckDbService.getSchema(leftId).map { it.name }
        val rightSchema = duckDbService.getSchema(rightId).map { it.name }
        val leftNulls = duckDbService.getNullColumns(leftId).toSet()
        val rightNulls = duckDbService.getNullColumns(rightId).toSet()
        val bothNull = leftNulls.intersect(rightNulls)

        // Determine which key columns were used (from the summary)
        val summary = duckDbService.getDataset(leftId) // we need keyColumns — stored in comparison views
        // Get value columns by looking at what views exist
        val commonColumns = leftSchema.intersect(rightSchema.toSet())

        val results = mutableListOf<ColumnChangeSummary>()
        for (col in commonColumns.filter { it !in bothNull }) {
            val viewName = "${compId}_col_${sanitize(col)}"
            try {
                val count = duckDbService.countTable(viewName)
                if (count > 0) {
                    results.add(ColumnChangeSummary(col, count))
                }
            } catch (_: Exception) {
                // View doesn't exist (probably a key column), skip
            }
        }
        return results.sortedByDescending { it.changedCount }
    }

    fun getColumnChangeData(leftId: Long, rightId: Long, column: String, page: Int, size: Int): DataPage {
        val viewName = "cmp_${leftId}_${rightId}_col_${sanitize(column)}"
        return duckDbService.queryView(viewName, page, size)
    }

    private fun q(col: String) = "\"${col.replace("\"", "\"\"")}\""

    private fun sanitize(col: String) = col.replace(Regex("[^a-zA-Z0-9_]"), "_").lowercase()
}
