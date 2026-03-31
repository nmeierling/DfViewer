package com.pmd.dfviewer.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "dfviewer")
data class DfViewerProperties(
    val cacheDir: String,
    val duckdbPath: String
)
