package net.snowflake.client.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.auth.BasicAWSCredentials;
import java.io.IOException;
import java.util.List;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.auth.wif.AwsAttestationService;
import net.snowflake.client.jdbc.BaseWiremockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CONNECTION)
public class PlatformDetectorLatestIT extends BaseWiremockTest {

  private AwsAttestationService mockAwsAttestationService;
  private EnvironmentProvider mockEnvironmentProvider;

  @BeforeEach
  public void setUp() {
    mockAwsAttestationService = mock(AwsAttestationService.class);
    mockEnvironmentProvider = mock(EnvironmentProvider.class);

    // Default behavior for environment variables (return null unless overridden)
    when(mockEnvironmentProvider.getEnv(anyString())).thenReturn(null);

    // Default behavior for AWS attestation service (return null/empty unless overridden)
    when(mockAwsAttestationService.getAWSCredentials()).thenReturn(null);

    resetWiremock();
  }

  @Test
  @DisplayName("Should detect AWS Lambda when LAMBDA_TASK_ROOT is set")
  public void testDetectAwsLambda() {
    // Arrange
    when(mockEnvironmentProvider.getEnv("LAMBDA_TASK_ROOT")).thenReturn("/var/task");

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act
    List<String> platforms = detector.detectPlatforms(0, mockAwsAttestationService);

    // Assert
    assertEquals(1, platforms.size(), "Should detect exactly 1 platform");
    assertTrue(
        platforms.contains("is_aws_lambda"),
        "Should detect AWS Lambda when LAMBDA_TASK_ROOT is set");
  }

  @Test
  @DisplayName("Should detect Azure Function when required environment variables are set")
  public void testDetectAzureFunction() {
    // Arrange
    when(mockEnvironmentProvider.getEnv("FUNCTIONS_WORKER_RUNTIME")).thenReturn("java");
    when(mockEnvironmentProvider.getEnv("FUNCTIONS_EXTENSION_VERSION")).thenReturn("~4");
    when(mockEnvironmentProvider.getEnv("AzureWebJobsStorage"))
        .thenReturn("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;");

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act
    List<String> platforms = detector.detectPlatforms(0, mockAwsAttestationService);

    // Assert
    assertEquals(1, platforms.size(), "Should detect exactly 1 platform");
    assertTrue(
        platforms.contains("is_azure_function"),
        "Should detect Azure Function when environment variables are set");
  }

  @Test
  @DisplayName("Should detect GCP Cloud Run Service when environment variables are set")
  public void testDetectGcpCloudRunService() {
    // Arrange
    when(mockEnvironmentProvider.getEnv("K_SERVICE")).thenReturn("my-service");
    when(mockEnvironmentProvider.getEnv("K_REVISION")).thenReturn("my-revision");
    when(mockEnvironmentProvider.getEnv("K_CONFIGURATION")).thenReturn("my-config");

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act
    List<String> platforms = detector.detectPlatforms(0, mockAwsAttestationService);

    // Assert
    assertEquals(1, platforms.size(), "Should detect exactly 1 platform");
    assertTrue(
        platforms.contains("is_gce_cloud_run_service"),
        "Should detect GCP Cloud Run Service when environment variables are set");
  }

  @Test
  @DisplayName("Should detect GCP Cloud Run Job when environment variables are set")
  public void testDetectGcpCloudRunJob() {
    // Arrange
    when(mockEnvironmentProvider.getEnv("CLOUD_RUN_JOB")).thenReturn("my-job");
    when(mockEnvironmentProvider.getEnv("CLOUD_RUN_EXECUTION")).thenReturn("my-execution");

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act
    List<String> platforms = detector.detectPlatforms(0, mockAwsAttestationService);

    // Assert
    assertEquals(1, platforms.size(), "Should detect exactly 1 platform");
    assertTrue(
        platforms.contains("is_gce_cloud_run_job"),
        "Should detect GCP Cloud Run Job when environment variables are set");
  }

  @Test
  @DisplayName("Should detect GitHub Action when GITHUB_ACTIONS is set")
  public void testDetectGithubAction() {
    // Arrange
    when(mockEnvironmentProvider.getEnv("GITHUB_ACTIONS")).thenReturn("true");

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act
    List<String> platforms = detector.detectPlatforms(0, mockAwsAttestationService);

    // Assert
    assertEquals(1, platforms.size(), "Should detect exactly 1 platform");
    assertTrue(
        platforms.contains("is_github_action"),
        "Should detect GitHub Action when GITHUB_ACTIONS is set");
  }

  @Test
  @DisplayName("Should detect EC2 instance with IMDSv2 using wiremock")
  public void testDetectEc2InstanceIMDSv2() throws IOException {
    // Arrange
    String mappingContent = loadMappingFile("platform-detection/ec2_successful_imdsv2");
    importMapping(mappingContent);

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act
    List<String> platforms = detector.detectPlatforms(500, mockAwsAttestationService);

    // Assert
    assertEquals(1, platforms.size(), "Should detect exactly 1 platform");
    assertTrue(platforms.contains("is_ec2_instance"), "Should detect EC2 instance using IMDSv2");
  }

  @Test
  @DisplayName("Should detect EC2 instance with IMDSv1 using wiremock")
  public void testDetectEc2InstanceIMDSv1() throws IOException {
    // Arrange
    String mappingContent = loadMappingFile("platform-detection/ec2_successful_imdsv1");
    importMapping(mappingContent);

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act
    List<String> platforms = detector.detectPlatforms(500, mockAwsAttestationService);

    // Assert
    assertEquals(1, platforms.size(), "Should detect exactly 1 platform");
    assertTrue(platforms.contains("is_ec2_instance"), "Should detect EC2 instance using IMDSv1");
  }

  @Test
  @DisplayName("Should detect Azure VM using wiremock")
  public void testDetectAzureVm() throws IOException {
    // Arrange
    String mappingContent = loadMappingFile("platform-detection/azure_vm_successful");
    importMapping(mappingContent);

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act
    List<String> platforms = detector.detectPlatforms(500, mockAwsAttestationService);

    // Assert
    assertEquals(1, platforms.size(), "Should detect exactly 1 platform");
    assertTrue(platforms.contains("is_azure_vm"), "Should detect Azure VM using wiremock");
  }

  @Test
  @DisplayName("Should detect Azure managed identity using wiremock")
  public void testDetectAzureManagedIdentity() throws IOException {
    // Arrange
    String mappingContent = loadMappingFile("platform-detection/azure_managed_identity_successful");
    importMapping(mappingContent);

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act
    List<String> platforms = detector.detectPlatforms(500, mockAwsAttestationService);

    // Assert
    assertEquals(1, platforms.size(), "Should detect exactly 1 platform");
    assertTrue(
        platforms.contains("has_azure_managed_identity"),
        "Should detect Azure managed identity using wiremock");
  }

  @Test
  @DisplayName("Should detect GCE VM using wiremock")
  public void testDetectGceVm() throws IOException {
    // Arrange
    String mappingContent = loadMappingFile("platform-detection/gcp_vm_successful");
    importMapping(mappingContent);

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act
    List<String> platforms = detector.detectPlatforms(500, mockAwsAttestationService);

    // Assert
    assertEquals(1, platforms.size(), "Should detect exactly 1 platform");
    assertTrue(platforms.contains("is_gce_vm"), "Should detect GCE VM using wiremock");
  }

  @Test
  @DisplayName("Should detect GCP identity using wiremock")
  public void testDetectGcpIdentity() throws IOException {
    // Arrange
    String mappingContent = loadMappingFile("platform-detection/gcp_identity_successful");
    importMapping(mappingContent);

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act
    List<String> platforms = detector.detectPlatforms(500, mockAwsAttestationService);

    // Assert
    assertEquals(1, platforms.size(), "Should detect exactly 1 platform");
    assertTrue(platforms.contains("has_gcp_identity"), "Should detect GCP identity using wiremock");
  }

  @Test
  @DisplayName("Should detect Azure managed identity for Functions with async execution")
  public void testAzureFunctionWithAsyncExecution() {
    // Arrange - Set up environment for Azure Function
    when(mockEnvironmentProvider.getEnv("FUNCTIONS_WORKER_RUNTIME")).thenReturn("java");
    when(mockEnvironmentProvider.getEnv("FUNCTIONS_EXTENSION_VERSION")).thenReturn("~4");
    when(mockEnvironmentProvider.getEnv("AzureWebJobsStorage"))
        .thenReturn("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;");
    when(mockEnvironmentProvider.getEnv("IDENTITY_HEADER")).thenReturn("test-header-value");

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act - Use timeout > 0 to trigger async execution
    List<String> platforms = detector.detectPlatforms(1000, mockAwsAttestationService);

    // Assert - Both Azure Function and managed identity should be detected
    assertTrue(
        platforms.size() == 2, "Should detect at least 2 platforms, but detected: " + platforms);
    assertTrue(
        platforms.contains("is_azure_function"), "Should detect Azure Function in async mode");
    assertTrue(
        platforms.contains("has_azure_managed_identity"),
        "Should detect Azure managed identity in async mode");
  }

  @Test
  @DisplayName("Should detect AWS identity when AWS attestation service returns valid identity")
  public void testDetectAwsIdentity() {
    // Arrange - Mock AWS attestation service to return valid credentials and ARN
    BasicAWSCredentials awsCredentials =
        // pragma: allowlist nextline secret
        new BasicAWSCredentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    when(mockAwsAttestationService.getAWSCredentials()).thenReturn(awsCredentials);
    when(mockAwsAttestationService.getCallerIdentityArn(awsCredentials, 200))
        .thenReturn("arn:aws:iam::123456789012:user/testuser");

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act
    List<String> platforms = detector.detectPlatforms(200, mockAwsAttestationService);

    // Assert
    assertEquals(1, platforms.size(), "Should detect exactly 1 platform");
    assertTrue(
        platforms.contains("has_aws_identity"),
        "Should detect AWS identity when attestation service returns valid credentials and ARN");
  }

  @Test
  @DisplayName("Should detect multiple platforms when conditions are met")
  public void testDetectMultiplePlatforms() throws IOException {
    // Arrange - Set up environment for AWS Lambda
    when(mockEnvironmentProvider.getEnv("LAMBDA_TASK_ROOT")).thenReturn("/var/task");

    // Set up wiremock for EC2 detection
    String mappingContent = loadMappingFile("platform-detection/ec2_successful_imdsv2");
    importMapping(mappingContent);
    // Ensure other metadata endpoints return immediately to avoid wiremock timeouts
    String gcpMetadataUnavailable = loadMappingFile("platform-detection/gcp_metadata_unavailable");
    importMapping(gcpMetadataUnavailable);
    String azureMetadataUnavailable =
        loadMappingFile("platform-detection/azure_metadata_unavailable");
    importMapping(azureMetadataUnavailable);

    BasicAWSCredentials awsCredentials =
        // pragma: allowlist nextline secret
        new BasicAWSCredentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    when(mockAwsAttestationService.getAWSCredentials()).thenReturn(awsCredentials);
    when(mockAwsAttestationService.getCallerIdentityArn(awsCredentials, 500))
        .thenReturn("arn:aws:sts::123456789012:assumed-role/test-role/session");

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act - Use small timeout to enable HTTP calls but ensure static mocks work in async context
    List<String> platforms = detector.detectPlatforms(500, mockAwsAttestationService);

    // Assert - Should detect multiple platforms
    assertEquals(3, platforms.size(), "Should detect exactly 3 platforms");
    assertTrue(
        platforms.contains("is_aws_lambda"), "Should detect AWS Lambda from environment variables");
    assertTrue(platforms.contains("is_ec2_instance"), "Should detect EC2 instance from wiremock");
    assertTrue(
        platforms.contains("has_aws_identity"),
        "Should detect AWS identity from attestation service");
  }

  @Test
  @DisplayName("Should handle timeout scenarios gracefully with no network calls")
  public void testTimeoutScenariosNoNetwork() {
    // Arrange - No mappings to simulate timeout
    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act - Short timeout to force timeouts
    List<String> platforms = detector.detectPlatforms(0, mockAwsAttestationService);

    // Assert - Should not contain any network-dependent platforms
    assertEquals(0, platforms.size(), "Should detect no platforms with 0ms timeout");
  }

  @Test
  @DisplayName("Should timeout reasonably when endpoints respond slowly")
  public void testSlowResponseTimeout() throws IOException {
    // Arrange - Load mapping with 5-second delay
    String mappingContent = loadMappingFile("platform-detection/slow_response_timeout");
    importMapping(mappingContent);

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act - Use 200ms timeout, but server takes 5 seconds
    long startTime = System.currentTimeMillis();
    List<String> platforms = detector.detectPlatforms(200, mockAwsAttestationService);
    long endTime = System.currentTimeMillis();
    long totalTime = endTime - startTime;

    // Assert - Should complete in reasonable time
    // Note: Due to HTTP client retries and parallel execution, actual time may be longer than
    // timeout
    // but should still be much less than the 5-second server delay
    assertTrue(
        totalTime < 20000,
        "Detection should complete much faster than server delay * number of http calls (5s), but took "
            + totalTime
            + "ms");

    // Should not detect any network-dependent platforms due to timeout, but may detect timeout
    // platforms
    assertEquals(5, platforms.size(), "Should detect only 5 timeouts: " + platforms);
    assertTrue(platforms.contains("is_ec2_instance_timeout"));
    assertTrue(platforms.contains("is_gce_vm_timeout"));
    assertTrue(platforms.contains("has_gcp_identity_timeout"));
    assertTrue(platforms.contains("is_azure_vm_timeout"));
    assertTrue(platforms.contains("is_ec2_instance_timeout"));
  }

  @Test
  @DisplayName("Should detect GCP VM when response is slower than timeout but still reasonable")
  public void testGcpVmSlowButWithinTimeout() throws IOException {
    // Arrange - Load mapping with 1-second delay but use 2-second timeout
    String mappingContent = loadMappingFile("platform-detection/gcp_vm_slow_response");
    importMapping(mappingContent);

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Act - Use 2000ms timeout, server takes 1 second
    List<String> platforms = detector.detectPlatforms(2000, mockAwsAttestationService);

    // Should detect GCP VM despite the delay
    assertEquals(1, platforms.size(), "Should detect exactly 1 platform");
    assertTrue(platforms.contains("is_gce_vm"), "Should detect GCE VM despite 1-second delay");
  }

  @Test
  @DisplayName("Should demonstrate env-only vs env + unreachable http endpoints time difference")
  public void testEnvVsEnvAndUnreachableEndpointsPerformance() {
    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    // Test 1: Fast timeout with no network (should be very fast)
    long startTime1 = System.currentTimeMillis();
    List<String> platforms1 = detector.detectPlatforms(0, mockAwsAttestationService);
    long timeNoNetwork = System.currentTimeMillis() - startTime1;

    // Test 2: Short timeout with unreachable endpoints
    long startTime2 = System.currentTimeMillis();
    List<String> platforms2 = detector.detectPlatforms(1000, mockAwsAttestationService);
    long timeUnreachableEndpoints = System.currentTimeMillis() - startTime2;

    // Assert performance characteristics
    assertTrue(
        timeNoNetwork < 100,
        "No network detection should be very fast, took " + timeNoNetwork + "ms");

    // With short timeout and unreachable endpoints, should still be reasonably fast
    // even with retries, much faster than if we waited for actual 5-second delays
    assertTrue(
        timeUnreachableEndpoints < 5000,
        "Unreachable endpoints should prevent long delays, took "
            + timeUnreachableEndpoints
            + "ms");

    // Both should return empty network-based platforms
    assertEquals(0, platforms1.size(), "No network test should detect no platforms");
    assertEquals(0, platforms2.size(), "Unreachable endpoints test should detect no platforms");
  }

  /** Helper method to load wiremock mapping files */
  private String loadMappingFile(String mappingName) throws IOException {
    String mappingFile = "src/test/resources/wiremock/mappings/" + mappingName + ".json";
    return new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(mappingFile)));
  }

  @Test
  @DisplayName("Should cache platform detection results")
  public void testCachedPlatformDetections() {
    when(mockEnvironmentProvider.getEnv("LAMBDA_TASK_ROOT")).thenReturn("/var/task");

    PlatformDetector detector =
        new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

    long startTime1 = System.currentTimeMillis();
    List<String> platforms1 =
        PlatformDetector.detectPlatformsAndCache(detector, mockAwsAttestationService);
    long duration1 = System.currentTimeMillis() - startTime1;

    long startTime2 = System.currentTimeMillis();
    List<String> platforms2 = PlatformDetector.getCachedPlatformDetection();
    long duration2 = System.currentTimeMillis() - startTime2;

    long startTime3 = System.currentTimeMillis();
    List<String> platforms3 = PlatformDetector.getCachedPlatformDetection();
    long duration3 = System.currentTimeMillis() - startTime3;

    assertSame(platforms1, platforms2, "Second call should return cached instance");
    assertSame(platforms2, platforms3, "Third call should return cached instance");

    assertTrue(
        duration2 < duration1 || duration2 < 10,
        "Cached call should be faster than initial detection. First: "
            + duration1
            + "ms, Second: "
            + duration2
            + "ms");
    assertTrue(
        duration3 < duration1 || duration3 < 10,
        "Cached call should be faster than initial detection. First: "
            + duration1
            + "ms, Third: "
            + duration3
            + "ms");
  }

  /** Get the base URL for wiremock */
  private String getBaseUrl() {
    return String.format("http://%s:%d", WIREMOCK_HOST, wiremockHttpPort);
  }
}
