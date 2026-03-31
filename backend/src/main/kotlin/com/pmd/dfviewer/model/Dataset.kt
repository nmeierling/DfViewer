package com.pmd.dfviewer.model

import java.time.Instant

data class Dataset(
    val id: Long,
    val name: String,
    val sourceUri: String,
    val sourceType: SourceType,
    val entityPath: String?,
    val runTimestamp: String?,
    val importedAt: Instant,
    val rowCount: Long,
    val schemaJson: String
)

enum class SourceType {
    S3_PARQUET,
    S3_CSV,
    LOCAL_PARQUET,
    LOCAL_CSV
}

data class ColumnInfo(
    val name: String,
    val type: String
)

data class DataPage(
    val data: List<Map<String, Any?>>,
    val totalRows: Long,
    val page: Int,
    val pageSize: Int,
    val totalPages: Int
)

data class ComparisonResult(
    val added: DataPage,
    val removed: DataPage,
    val changed: DataPage,
    val summary: ComparisonSummary
)

data class ComparisonSummary(
    val totalLeft: Long,
    val totalRight: Long,
    val addedCount: Long,
    val removedCount: Long,
    val changedCount: Long,
    val unchangedCount: Long
)
