package net.snowflake.client.util;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.auth.wif.AwsAttestationService;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;

import net.snowflake.client.core.auth.wif.PlatformDetectionUtil;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.util.Collections;

@SnowflakeJdbcInternalApi
public class PlatformDetector {
    
    private static final SFLogger logger = SFLoggerFactory.getLogger(PlatformDetector.class);

    // AWS platform detection constants
    private static final String AWS_LAMBDA_TASK_ROOT = "LAMBDA_TASK_ROOT";
    
    // Azure platform detection constants
    private static final String AZURE_FUNCTIONS_WORKER_RUNTIME = "FUNCTIONS_WORKER_RUNTIME";
    private static final String AZURE_FUNCTIONS_EXTENSION_VERSION = "FUNCTIONS_EXTENSION_VERSION";
    private static final String AZURE_WEBJOBS_STORAGE = "AzureWebJobsStorage";
    private static final String AZURE_IDENTITY_HEADER = "IDENTITY_HEADER";
    
    // GCP platform detection constants
    private static final String GCP_K_SERVICE = "K_SERVICE";
    private static final String GCP_K_REVISION = "K_REVISION";
    private static final String GCP_K_CONFIGURATION = "K_CONFIGURATION";
    private static final String GCP_CLOUD_RUN_JOB = "CLOUD_RUN_JOB";
    private static final String GCP_CLOUD_RUN_EXECUTION = "CLOUD_RUN_EXECUTION";
    
    // GitHub Actions detection constants
    private static final String GITHUB_ACTIONS = "GITHUB_ACTIONS";

    // Default cloud metadata service URLs
    private static final String DEFAULT_METADATA_SERVICE_BASE_URL = "http://169.254.169.254";
    private static final String DEFAULT_GCP_METADATA_BASE_URL = "http://metadata.google.internal";
    
    // Metadata service headers
    private static final String AWS_METADATA_TOKEN_TTL_HEADER = "X-aws-ec2-metadata-token-ttl-seconds";
    private static final String AWS_METADATA_TOKEN_HEADER = "X-aws-ec2-metadata-token";
    private static final String AZURE_METADATA_HEADER = "Metadata";
    private static final String GCP_METADATA_FLAVOR_HEADER = "Metadata-Flavor";
    private static final String GCP_METADATA_FLAVOR_VALUE = "Google";

    // Instance fields for configurable URLs and environment provider (for testing)
    private final String awsMetadataBaseUrl;
    private final String azureMetadataBaseUrl;
    private final String gcpMetadataBaseUrl;
    private final EnvironmentProvider environmentProvider;

    // Default constructor for production use
    public PlatformDetector() {
        this.awsMetadataBaseUrl = DEFAULT_METADATA_SERVICE_BASE_URL;
        this.azureMetadataBaseUrl = DEFAULT_METADATA_SERVICE_BASE_URL;
        this.gcpMetadataBaseUrl = DEFAULT_GCP_METADATA_BASE_URL;
        this.environmentProvider = SnowflakeEnvironmentProvider.INSTANCE;
    }

    /** Constructor for testing purposes - allows overriding both URLs and environment provider */
    PlatformDetector(String awsMetadataBaseUrl, String azureMetadataBaseUrl, String gcpMetadataBaseUrl, 
                    EnvironmentProvider environmentProvider) {
        this.awsMetadataBaseUrl = awsMetadataBaseUrl;
        this.azureMetadataBaseUrl = azureMetadataBaseUrl;
        this.gcpMetadataBaseUrl = gcpMetadataBaseUrl;
        this.environmentProvider = environmentProvider;
    }

    private enum DetectionState {
        DETECTED,
        NOT_DETECTED, 
        TIMEOUT
    }

    /**
     * Detect all potential platforms that the current environment may be running on.
     * Swallows all exceptions and returns an empty list if any exception occurs.
     *
     * @param platformDetectionTimeoutMs Timeout value for platform detection requests in milliseconds.
     *                                  If null, defaults to 200 milliseconds.
     *                                  If 0, skips network-dependent checks.
     * @return List of detected platform names. Platforms that timed out will have
     *         "_timeout" suffix appended to their name. Returns empty list if any
     *         exception occurs during detection.
     */
    public List<String> detectPlatforms(Integer platformDetectionTimeoutMs, AwsAttestationService attestationService) {
        try {
            int timeoutMs = platformDetectionTimeoutMs != null ? platformDetectionTimeoutMs : 200;

            // Run environment-only checks synchronously (no network calls)
            Map<Platform, DetectionState> platforms = new HashMap<>();
            platforms.put(Platform.IS_AWS_LAMBDA, isAwsLambda());
            platforms.put(Platform.IS_AZURE_FUNCTION, isAzureFunction());
            platforms.put(Platform.IS_GCE_CLOUD_RUN_SERVICE, isGcpCloudRunService());
            platforms.put(Platform.IS_GCE_CLOUD_RUN_JOB, isGcpCloudRunJob());
            platforms.put(Platform.IS_GITHUB_ACTION, isGithubAction());

            if (timeoutMs != 0) {
                ExecutorService executor = Executors.newFixedThreadPool(6);
                try {
                    Map<Platform, CompletableFuture<DetectionState>> futures = new HashMap<>();

                    futures.put(Platform.IS_EC2_INSTANCE,
                            CompletableFuture.supplyAsync(() -> isEc2Instance(timeoutMs), executor));
                    futures.put(Platform.HAS_AWS_IDENTITY,
                            CompletableFuture.supplyAsync(() -> hasAwsIdentity(attestationService), executor));
                    futures.put(Platform.IS_AZURE_VM,
                            CompletableFuture.supplyAsync(() -> isAzureVm(timeoutMs), executor));
                    futures.put(Platform.HAS_AZURE_MANAGED_IDENTITY,
                            CompletableFuture.supplyAsync(() -> hasAzureManagedIdentity(timeoutMs), executor));
                    futures.put(Platform.IS_GCE_VM,
                            CompletableFuture.supplyAsync(() -> isGceVm(timeoutMs), executor));
                    futures.put(Platform.HAS_GCP_IDENTITY,
                            CompletableFuture.supplyAsync(() -> hasGcpIdentity(timeoutMs), executor));

                    // Wait for all futures to complete with timeout
                    for (Map.Entry<Platform, CompletableFuture<DetectionState>> entry : futures.entrySet()) {
                        try {
                            DetectionState result = entry.getValue().get((long) timeoutMs, TimeUnit.MILLISECONDS);
                            platforms.put(entry.getKey(), result);
                        } catch (TimeoutException e) {
                            logger.debug("Platform detection timed out for: {}", entry.getKey());
                            platforms.put(entry.getKey(), DetectionState.TIMEOUT);
                        } catch (Exception e) {
                            logger.debug("Platform detection failed for {}: {}", entry.getKey(), e.getMessage());
                            platforms.put(entry.getKey(), DetectionState.NOT_DETECTED);
                        }
                    }

                } finally {
                    executor.shutdown();
                    try {
                        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                            executor.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        executor.shutdownNow();
                        Thread.currentThread().interrupt();
                    }
                }
            }

            List<String> detectedPlatforms = getDetectedPlatforms(platforms);

            logger.debug("Platform detection completed. Detected platforms: {}", detectedPlatforms);
            return detectedPlatforms;

        } catch (Exception e) {
            logger.debug("Platform detection failed with exception: {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    private static List<String> getDetectedPlatforms(Map<Platform, DetectionState> platforms) {
        List<String> detectedPlatforms = new ArrayList<>();
        for (Map.Entry<Platform, DetectionState> entry : platforms.entrySet()) {
            Platform platform = entry.getKey();
            DetectionState state = entry.getValue();

            if (state == DetectionState.DETECTED) {
                detectedPlatforms.add(platform.getValue());
            } else if (state == DetectionState.TIMEOUT) {
                detectedPlatforms.add(platform.getValue() + "_timeout");
            }
            // NOT_DETECTED platforms are not included in the result list
        }
        return detectedPlatforms;
    }

    private static boolean isTimeoutException(Exception e) {
        if (e instanceof SocketTimeoutException) {
            return true;
        }
        if (e instanceof TimeoutException) {
            return true;
        }
        if (e instanceof SnowflakeSQLException) {
            String message = e.getMessage();
            return message != null && (
                message.contains("timeout") || 
                message.contains("timed out") ||
                message.contains("elapsed time") ||
                message.toLowerCase().contains("timeout")
            );
        }
        // Check for nested timeout exceptions
        Throwable cause = e.getCause();
        if (cause instanceof SocketTimeoutException || cause instanceof TimeoutException) {
            return true;
        }
        return false;
    }
    

    private static String executeHttpGet(String uri, Map<String, String> headers, int timeoutMs, String method) 
        throws SnowflakeSQLException, IOException {
        HttpGet request = new HttpGet(uri);
        
        // Add headers in pairs (key, value)
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            request.setHeader(entry.getKey(), entry.getValue());
        }
        
        return PlatformDetectionUtil.performPlatformDetectionRequest(request, timeoutMs / 1000.0);
    }

    private static boolean checkHttpGetSuccess(String uri, int timeoutMs, Map<String, String> headers) {
        try {
            executeHttpGet(uri, headers, timeoutMs, "GET");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private DetectionState isEc2Instance(int timeoutMs) {
        try {
            // First try to get IMDSv2 token
            String token = null;
            try {
                String tokenResponse = executeHttpGet(
                    getAwsMetadataTokenEndpoint(),
                    Collections.singletonMap("X-aws-ec2-metadata-token-ttl-seconds", "21600"),
                    timeoutMs,
                    "PUT");
                if (tokenResponse != null && !tokenResponse.trim().isEmpty()) {
                    token = tokenResponse.trim();
                    logger.debug("Successfully obtained IMDSv2 token");
                }
            } catch (Exception e) {
                logger.debug("Failed to get IMDSv2 token, will try IMDSv1: {}", e.getMessage());
            }

            // Try to get instance identity document
            Map<String, String> headers = new HashMap<>();
            if (token != null) {
                headers.put("X-aws-ec2-metadata-token", token);
            }

            String response = executeHttpGet(getAwsMetadataIdentityEndpoint(), headers, timeoutMs, "GET");
            if (response != null && response.contains("instanceId")) {
                logger.debug("Successfully detected EC2 instance via metadata service");
                return DetectionState.DETECTED;
            }
        } catch (Exception e) {
            logger.debug("EC2 instance detection failed: {}", e.getMessage());
            if (isTimeoutException(e)) {
                return DetectionState.TIMEOUT;
            }
        }
        return DetectionState.NOT_DETECTED;
    }

    private DetectionState isAwsLambda() {
        return checkAllEnvironmentVariables(AWS_LAMBDA_TASK_ROOT)
            ? DetectionState.DETECTED 
            : DetectionState.NOT_DETECTED;
    }

    private static DetectionState hasAwsIdentity(AwsAttestationService attestationService) {
        return PlatformDetectionUtil.hasValidAwsIdentityForWif(attestationService) 
            ? DetectionState.DETECTED 
            : DetectionState.NOT_DETECTED;
    }

    private DetectionState isAzureVm(int timeoutMs) {
        try {
            Map<String, String> headers = Collections.singletonMap("Metadata", "true");
            String response = executeHttpGet(getAzureMetadataInstanceEndpoint(), headers, timeoutMs, "GET");
            if (response != null && response.contains("vmId")) {
                logger.debug("Successfully detected Azure VM via metadata service");
                return DetectionState.DETECTED;
            }
        } catch (Exception e) {
            logger.debug("Azure VM detection failed: {}", e.getMessage());
            if (isTimeoutException(e)) {
                return DetectionState.TIMEOUT;
            }
        }
        return DetectionState.NOT_DETECTED;
    }

    private DetectionState isAzureFunction() {
        return checkAllEnvironmentVariables(AZURE_FUNCTIONS_WORKER_RUNTIME, AZURE_FUNCTIONS_EXTENSION_VERSION, AZURE_WEBJOBS_STORAGE)
            ? DetectionState.DETECTED 
            : DetectionState.NOT_DETECTED;
    }

    private DetectionState isManagedIdentityAvailableOnAzureVm(int timeoutMs, String resource) {
        try {
            String endpoint = getAzureManagedIdentityEndpoint(resource);
            String response = executeHttpGet(endpoint, Collections.emptyMap(), timeoutMs, "GET");
            if (response != null && response.contains("access_token")) {
                logger.debug("Successfully detected Azure managed identity");
                return DetectionState.DETECTED;
            }
        } catch (Exception e) {
            logger.debug("Azure managed identity detection failed: {}", e.getMessage());
            if (isTimeoutException(e)) {
                return DetectionState.TIMEOUT;
            }
        }
        return DetectionState.NOT_DETECTED;
    }

    private DetectionState hasAzureManagedIdentity(int timeoutMs) {
        // Check environment variable first (for Azure Functions)
        if (checkAllEnvironmentVariables("IDENTITY_HEADER")) {
            logger.debug("Detected Azure managed identity via IDENTITY_HEADER environment variable");
            return DetectionState.DETECTED;
        }

        // Check via metadata service (for Azure VMs)
        return isManagedIdentityAvailableOnAzureVm(timeoutMs, "https://management.azure.com");
    }

    private DetectionState isGceVm(int timeoutMs) {
        try {
            Map<String, String> headers = Collections.singletonMap("Metadata-Flavor", "Google");
            String response = executeHttpGet(getGcpMetadataBaseEndpoint(), headers, timeoutMs, "GET");
            if (response != null && (response.contains("computeMetadata") || response.contains("0.1/"))) {
                logger.debug("Successfully detected GCE VM via metadata service");
                return DetectionState.DETECTED;
            }
        } catch (Exception e) {
            logger.debug("GCE VM detection failed: {}", e.getMessage());
            if (isTimeoutException(e)) {
                return DetectionState.TIMEOUT;
            }
        }
        return DetectionState.NOT_DETECTED;
    }

    private DetectionState isGcpCloudRunService() {
        return checkAllEnvironmentVariables(GCP_K_SERVICE, GCP_K_REVISION, GCP_K_CONFIGURATION)
            ? DetectionState.DETECTED 
            : DetectionState.NOT_DETECTED;
    }

    private DetectionState isGcpCloudRunJob() {
        return checkAllEnvironmentVariables(GCP_CLOUD_RUN_JOB, GCP_CLOUD_RUN_EXECUTION)
            ? DetectionState.DETECTED 
            : DetectionState.NOT_DETECTED;
    }

    private DetectionState hasGcpIdentity(int timeoutMs) {
        try {
            Map<String, String> headers = Collections.singletonMap("Metadata-Flavor", "Google");
            String response = executeHttpGet(getGcpServiceAccountEndpoint(), headers, timeoutMs, "GET");
            if (response != null && response.contains("@")) {
                logger.debug("Successfully detected GCP identity via metadata service");
                return DetectionState.DETECTED;
            }
        } catch (Exception e) {
            logger.debug("GCP identity detection failed: {}", e.getMessage());
            if (isTimeoutException(e)) {
                return DetectionState.TIMEOUT;
            }
        }
        return DetectionState.NOT_DETECTED;
    }

    private DetectionState isGithubAction() {
        return checkAllEnvironmentVariables(GITHUB_ACTIONS)
            ? DetectionState.DETECTED 
            : DetectionState.NOT_DETECTED;
    }

    private boolean checkAllEnvironmentVariables(String... variableNames) {
        if (variableNames == null || variableNames.length == 0) {
            return false;
        }

        for (String varName : variableNames) {
            String value = environmentProvider.getEnv(varName);
            if (value == null || value.trim().isEmpty()) {
                logger.debug("Environment variable {} is not set or empty", varName);
                return false;
            }
        }

        logger.debug("All environment variables are present: {}", java.util.Arrays.toString(variableNames));
        return true;
    }


    private String getAwsMetadataTokenEndpoint() {
        return awsMetadataBaseUrl + "/latest/api/token";
    }

    private String getAwsMetadataIdentityEndpoint() {
        return awsMetadataBaseUrl + "/latest/dynamic/instance-identity/document";
    }

    private String getAzureMetadataInstanceEndpoint() {
        return azureMetadataBaseUrl + "/metadata/instance?api-version=2021-02-01";
    }

    private String getAzureManagedIdentityEndpoint(String resource) {
        return azureMetadataBaseUrl + "/metadata/identity/oauth2/token?api-version=2018-02-01&resource=" + resource;
    }

    private String getGcpMetadataBaseEndpoint() {
        return gcpMetadataBaseUrl + "/computeMetadata/v1/instance";
    }

    private String getGcpServiceAccountEndpoint() {
        return gcpMetadataBaseUrl + "/computeMetadata/v1/instance/service-accounts/default/email";
    }
} 