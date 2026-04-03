package com.pmd.dfviewer.model

import java.time.Instant

data class ColumnJoinConfig(
    val sourceColumn: String,
    val joinDatasetId: Long,
    val joinColumn: String,
    val displayTemplate: String,  // e.g. "{name} ({code})" — fields from join dataset
    val mode: String = "replace"  // "replace" or "add"
)

data class Dataset(
    val id: Long,
    val name: String,
    val sourceUri: String,
    val sourceType: SourceType,
    val entityPath: String?,
    val runTimestamp: String?,
    val importedAt: Instant,
    val rowCount: Long,
    val sizeBytes: Long,
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

data class CompareRequest(
    val leftDatasetId: Long,
    val rightDatasetId: Long,
    val keyColumns: List<String>,
    val ignoreColumns: List<String> = emptyList()
)

data class ComparisonSummary(
    val leftDatasetId: Long,
    val rightDatasetId: Long,
    val leftName: String,
    val rightName: String,
    val keyColumns: List<String>,
    val totalLeft: Long,
    val totalRight: Long,
    val addedCount: Long,
    val removedCount: Long,
    val changedCount: Long,
    val unchangedCount: Long
)

data class ChangedRow(
    val key: Map<String, Any?>,
    val changes: Map<String, Pair<Any?, Any?>>  // column -> (left, right)
)
