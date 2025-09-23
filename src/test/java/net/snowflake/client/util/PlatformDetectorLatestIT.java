package net.snowflake.client.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import net.snowflake.client.core.auth.wif.AwsAttestationService;
import net.snowflake.client.jdbc.BaseWiremockTest;
import com.amazonaws.auth.BasicAWSCredentials;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

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
        when(mockAwsAttestationService.getArn()).thenReturn(null);
    }

    @Test
    @DisplayName("Should detect AWS Lambda when LAMBDA_TASK_ROOT is set")
    public void testDetectAwsLambda() {
        // Arrange
        when(mockEnvironmentProvider.getEnv("LAMBDA_TASK_ROOT")).thenReturn("/var/task");
        
        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act
        List<String> platforms = detector.detectPlatforms(0, mockAwsAttestationService);

        // Assert
        assertTrue(platforms.contains("is_aws_lambda"), 
                "Should detect AWS Lambda when LAMBDA_TASK_ROOT is set");
    }

    @Test
    @DisplayName("Should not detect AWS Lambda when LAMBDA_TASK_ROOT is not set")
    public void testNotDetectAwsLambda() {
        // Arrange - mockEnvironmentProvider already returns null by default
        
        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act
        List<String> platforms = detector.detectPlatforms(0, mockAwsAttestationService);

        // Assert
        assertFalse(platforms.contains("is_aws_lambda"), 
                "Should not detect AWS Lambda when LAMBDA_TASK_ROOT is not set");
    }

    @Test
    @DisplayName("Should detect Azure Function when required environment variables are set")
    public void testDetectAzureFunction() {
        // Arrange
        when(mockEnvironmentProvider.getEnv("FUNCTIONS_WORKER_RUNTIME")).thenReturn("java");
        when(mockEnvironmentProvider.getEnv("FUNCTIONS_EXTENSION_VERSION")).thenReturn("~4");
        when(mockEnvironmentProvider.getEnv("AzureWebJobsStorage")).thenReturn("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;");
        
        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act
        List<String> platforms = detector.detectPlatforms(0, mockAwsAttestationService);

        // Assert
        assertTrue(platforms.contains("is_azure_function"), 
                "Should detect Azure Function when environment variables are set");
    }

    @Test
    @DisplayName("Should detect GCP Cloud Run Service when environment variables are set")
    public void testDetectGcpCloudRunService() {
        // Arrange
        when(mockEnvironmentProvider.getEnv("K_SERVICE")).thenReturn("my-service");
        when(mockEnvironmentProvider.getEnv("K_REVISION")).thenReturn("my-revision");
        when(mockEnvironmentProvider.getEnv("K_CONFIGURATION")).thenReturn("my-config");
        
        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act
        List<String> platforms = detector.detectPlatforms(0, mockAwsAttestationService);

        // Assert
        assertTrue(platforms.contains("is_gce_cloud_run_service"), 
                "Should detect GCP Cloud Run Service when environment variables are set");
    }

    @Test
    @DisplayName("Should detect GCP Cloud Run Job when environment variables are set")
    public void testDetectGcpCloudRunJob() {
        // Arrange
        when(mockEnvironmentProvider.getEnv("CLOUD_RUN_JOB")).thenReturn("my-job");
        when(mockEnvironmentProvider.getEnv("CLOUD_RUN_EXECUTION")).thenReturn("my-execution");
        
        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act
        List<String> platforms = detector.detectPlatforms(0, mockAwsAttestationService);

        // Assert
        assertTrue(platforms.contains("is_gce_cloud_run_job"), 
                "Should detect GCP Cloud Run Job when environment variables are set");
    }

    @Test
    @DisplayName("Should detect GitHub Action when GITHUB_ACTIONS is set")
    public void testDetectGithubAction() {
        // Arrange
        when(mockEnvironmentProvider.getEnv("GITHUB_ACTIONS")).thenReturn("true");
        
        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act
        List<String> platforms = detector.detectPlatforms(0, mockAwsAttestationService);

        // Assert
        assertTrue(platforms.contains("is_github_action"), 
                "Should detect GitHub Action when GITHUB_ACTIONS is set");
    }

    @Test
    @DisplayName("Should detect EC2 instance with IMDSv2 using wiremock")
    public void testDetectEc2InstanceIMDSv2() throws IOException {
        // Arrange
        String mappingContent = loadMappingFile("platform-detection/ec2_successful_imdsv2");
        importMapping(mappingContent);

        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act
        List<String> platforms = detector.detectPlatforms(500, mockAwsAttestationService);

        // Assert
        assertTrue(platforms.contains("is_ec2_instance"), 
                "Should detect EC2 instance using IMDSv2");
    }

    @Test
    @DisplayName("Should detect EC2 instance with IMDSv1 using wiremock")
    public void testDetectEc2InstanceIMDSv1() throws IOException {
        // Arrange
        String mappingContent = loadMappingFile("platform-detection/ec2_successful_imdsv1");
        importMapping(mappingContent);

        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act
        List<String> platforms = detector.detectPlatforms(500, mockAwsAttestationService);

        // Assert
        assertTrue(platforms.contains("is_ec2_instance"), 
                "Should detect EC2 instance using IMDSv1");
    }

    @Test
    @DisplayName("Should detect Azure VM using wiremock")
    public void testDetectAzureVm() throws IOException {
        // Arrange
        String mappingContent = loadMappingFile("platform-detection/azure_vm_successful");
        importMapping(mappingContent);

        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act
        List<String> platforms = detector.detectPlatforms(500, mockAwsAttestationService);

        // Assert
        assertTrue(platforms.contains("is_azure_vm"), 
                "Should detect Azure VM using wiremock");
    }

    @Test
    @DisplayName("Should detect Azure managed identity using wiremock")
    public void testDetectAzureManagedIdentity() throws IOException {
        // Arrange
        String mappingContent = loadMappingFile("platform-detection/azure_managed_identity_successful");
        importMapping(mappingContent);

        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act
        List<String> platforms = detector.detectPlatforms(500, mockAwsAttestationService);

        // Assert
        assertTrue(platforms.contains("has_azure_managed_identity"), 
                "Should detect Azure managed identity using wiremock");
    }

    @Test
    @DisplayName("Should detect GCE VM using wiremock")
    public void testDetectGceVm() throws IOException {
        // Arrange
        String mappingContent = loadMappingFile("platform-detection/gcp_vm_successful");
        importMapping(mappingContent);

        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act
        List<String> platforms = detector.detectPlatforms(500, mockAwsAttestationService);

        // Assert
        assertTrue(platforms.contains("is_gce_vm"), 
                "Should detect GCE VM using wiremock");
    }

    @Test
    @DisplayName("Should detect GCP identity using wiremock")
    public void testDetectGcpIdentity() throws IOException {
        // Arrange
        String mappingContent = loadMappingFile("platform-detection/gcp_identity_successful");
        importMapping(mappingContent);

        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act
        List<String> platforms = detector.detectPlatforms(500, mockAwsAttestationService);

        // Assert
        assertTrue(platforms.contains("has_gcp_identity"), 
                "Should detect GCP identity using wiremock");
    }

    @Test
    @DisplayName("Should detect Azure managed identity for Functions with async execution")
    public void testAzureFunctionWithAsyncExecution() {
        // Arrange - Set up environment for Azure Function
        when(mockEnvironmentProvider.getEnv("FUNCTIONS_WORKER_RUNTIME")).thenReturn("java");
        when(mockEnvironmentProvider.getEnv("FUNCTIONS_EXTENSION_VERSION")).thenReturn("~4");
        when(mockEnvironmentProvider.getEnv("AzureWebJobsStorage")).thenReturn("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;");
        when(mockEnvironmentProvider.getEnv("IDENTITY_HEADER")).thenReturn("test-header-value");

        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act - Use timeout > 0 to trigger async execution
        List<String> platforms = detector.detectPlatforms(100, mockAwsAttestationService);

        // Assert - Both Azure Function and managed identity should be detected
        assertTrue(platforms.contains("is_azure_function"), 
                "Should detect Azure Function in async mode");
        assertTrue(platforms.contains("has_azure_managed_identity"), 
                "Should detect Azure managed identity in async mode");
    }

    @Test
    @DisplayName("Should detect AWS identity when AWS attestation service returns valid identity")
    public void testDetectAwsIdentity() {
        // Arrange - Mock AWS attestation service to return valid credentials and ARN
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        when(mockAwsAttestationService.getAWSCredentials()).thenReturn(awsCredentials);
        when(mockAwsAttestationService.getArn()).thenReturn("arn:aws:iam::123456789012:user/ExampleUser");

        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act
        List<String> platforms = detector.detectPlatforms(200, mockAwsAttestationService);

        // Assert
        assertTrue(platforms.contains("has_aws_identity"), 
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
        
        // Set up AWS identity mock - Mock attestation service to return valid credentials and ARN
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        when(mockAwsAttestationService.getAWSCredentials()).thenReturn(awsCredentials);
        when(mockAwsAttestationService.getArn()).thenReturn("arn:aws:iam::123456789012:user/ExampleUser");

        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act - Use small timeout to enable HTTP calls but ensure static mocks work in async context
        List<String> platforms = detector.detectPlatforms(100, mockAwsAttestationService);

        // Assert - Should detect multiple platforms
        assertTrue(platforms.contains("is_aws_lambda"), 
                "Should detect AWS Lambda from environment variables");
        assertTrue(platforms.contains("is_ec2_instance"), 
                "Should detect EC2 instance from wiremock");
        assertTrue(platforms.contains("has_aws_identity"), 
                "Should detect AWS identity from attestation service");
    }

    @Test
    @DisplayName("Should handle timeout scenarios gracefully")
    public void testTimeoutScenarios() throws IOException {
        // Arrange - No mappings to simulate timeout
        PlatformDetector detector = new PlatformDetector(getBaseUrl(), getBaseUrl(), getBaseUrl(), mockEnvironmentProvider);

        // Act - Short timeout to force timeouts
        List<String> platforms = detector.detectPlatforms(0, mockAwsAttestationService);

        // Assert - Should not contain any network-dependent platforms
        assertFalse(platforms.contains("is_ec2_instance"), 
                "Should not detect EC2 instance on timeout");
        assertFalse(platforms.contains("is_azure_vm"), 
                "Should not detect Azure VM on timeout");
        assertFalse(platforms.contains("is_gce_vm"), 
                "Should not detect GCE VM on timeout");
    }

    /**
     * Helper method to load wiremock mapping files
     */
    private String loadMappingFile(String mappingName) throws IOException {
        String mappingFile = "src/test/resources/wiremock/mappings/" + mappingName + ".json";
        return new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(mappingFile)));
    }

    /**
     * Get the base URL for wiremock
     */
    private String getBaseUrl() {
        return String.format("http://%s:%d", WIREMOCK_HOST, wiremockHttpPort);
    }
} 