package com.pmd.dfviewer.model

data class S3Credentials(
    val accessKeyId: String,
    val secretAccessKey: String,
    val sessionToken: String?,
    val region: String = "eu-central-1"
)

data class S3ScanResult(
    val uri: String,
    val files: List<S3FileEntry>,
    val etlRuns: List<EtlRunGroup>
)

data class S3FileEntry(
    val key: String,
    val uri: String,
    val size: Long,
    val type: FileType,
    val lastModified: String?
)

enum class FileType {
    PARQUET, CSV, UNKNOWN
}

data class EtlRunGroup(
    val entityPath: String,
    val runs: List<EtlRun>
)

data class EtlRun(
    val timestamp: String,
    val files: List<S3FileEntry>,
    val totalSize: Long
)

data class S3ImportRequest(
    val files: List<String>,
    val name: String,
    val entityPath: String? = null,
    val runTimestamp: String? = null
)

data class ScanProgress(
    val phase: String,
    val objectsScanned: Int,
    val dataFilesFound: Int,
    val message: String
)

data class ImportProgress(
    val phase: String,
    val fileIndex: Int,
    val totalFiles: Int,
    val fileName: String,
    val bytesDownloaded: Long,
    val bytesTotal: Long,
    val datasetId: Long? = null,
    val error: String? = null,
    val done: Boolean = false
)
