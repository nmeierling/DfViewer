package com.pmd.dfviewer.service

import com.pmd.dfviewer.model.S3Credentials
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

@Service
class S3CredentialService {

    private val log = LoggerFactory.getLogger(javaClass)

    @Volatile
    private var credentials: S3Credentials? = null

    @Volatile
    private var s3Client: S3Client? = null

    init {
        val envKey = System.getenv("AWS_ACCESS_KEY_ID")
        val envSecret = System.getenv("AWS_SECRET_ACCESS_KEY")
        val envToken = System.getenv("AWS_SESSION_TOKEN")
        val envRegion = System.getenv("AWS_REGION") ?: "eu-central-1"

        if (!envKey.isNullOrBlank() && !envSecret.isNullOrBlank()) {
            log.info("Loading S3 credentials from environment variables")
            configure(S3Credentials(envKey, envSecret, envToken, envRegion))
        }
    }

    fun configure(creds: S3Credentials) {
        this.credentials = creds
        this.s3Client?.close()

        val awsCreds = if (creds.sessionToken != null) {
            AwsSessionCredentials.create(creds.accessKeyId, creds.secretAccessKey, creds.sessionToken)
        } else {
            AwsSessionCredentials.create(creds.accessKeyId, creds.secretAccessKey, "")
        }

        this.s3Client = S3Client.builder()
            .region(Region.of(creds.region))
            .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
            .build()

        log.info("S3 credentials configured for region ${creds.region}")
    }

    fun getClient(): S3Client {
        return s3Client ?: throw IllegalStateException("S3 credentials not configured")
    }

    fun isConfigured(): Boolean = s3Client != null

    fun getCredentials(): S3Credentials? = credentials
}
