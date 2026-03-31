plugins {
    id("org.springframework.boot") version "3.4.4"
    id("io.spring.dependency-management") version "1.1.7"
    kotlin("jvm") version "2.1.10"
    kotlin("plugin.spring") version "2.1.10"
}

group = "com.pmd"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-websocket")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    // DuckDB
    implementation("org.duckdb:duckdb_jdbc:1.2.1")

    // AWS S3
    implementation("software.amazon.awssdk:s3:2.31.9")
    implementation("software.amazon.awssdk:sts:2.31.9")

    // Apache Arrow + Parquet reading
    implementation("org.apache.parquet:parquet-hadoop:1.15.1")
    implementation("org.apache.hadoop:hadoop-common:3.4.1") {
        exclude(group = "org.slf4j")
        exclude(group = "ch.qos.logback")
    }
    implementation("org.apache.hadoop:hadoop-client:3.4.1") {
        exclude(group = "org.slf4j")
        exclude(group = "ch.qos.logback")
    }

    // CSV
    implementation("org.apache.commons:commons-csv:1.12.0")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}
