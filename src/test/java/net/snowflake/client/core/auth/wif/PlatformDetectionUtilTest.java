package net.snowflake.client.core.auth.wif;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class PlatformDetectionUtilTest {

  private AwsAttestationService mockAttestationService;
  private AWSCredentials mockCredentials;

  @BeforeEach
  public void setUp() {
    mockAttestationService = mock(AwsAttestationService.class);
    mockCredentials = mock(AWSCredentials.class);
  }

  @Nested
  @DisplayName("hasValidAwsIdentityForWif Tests")
  class HasValidAwsIdentityForWifTests {

    @Test
    @DisplayName("Should return false when credentials are null")
    public void testNullCredentials() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(null);

      // Act
      boolean result =
          PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService, 1000);

      // Assert
      assertFalse(result, "Should return false when credentials are null");
    }

    @Test
    @DisplayName("Should return false when access key is null")
    public void testNullAccessKey() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn(null);
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); // pragma: allowlist secret

      // Act
      boolean result =
          PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService, 1000);

      // Assert
      assertFalse(result, "Should return false when access key is null");
    }

    @Test
    @DisplayName("Should return false when secret key is null")
    public void testNullSecretKey() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId())
          .thenReturn("AKIAIOSFODNN7EXAMPLE"); // pragma: allowlist secret
      when(mockCredentials.getAWSSecretKey()).thenReturn(null);

      // Act
      boolean result =
          PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService, 1000);

      // Assert
      assertFalse(result, "Should return false when secret key is null");
    }

    @Test
    @DisplayName("Should return false when access key is empty")
    public void testEmptyAccessKey() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("");
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); // pragma: allowlist secret

      // Act
      boolean result =
          PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService, 1000);

      // Assert
      assertFalse(result, "Should return false when access key is empty");
    }

    @Test
    @DisplayName("Should return false when secret key is empty")
    public void testEmptySecretKey() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId())
          .thenReturn("AKIAIOSFODNN7EXAMPLE"); // pragma: allowlist secret
      when(mockCredentials.getAWSSecretKey()).thenReturn("");

      // Act
      boolean result =
          PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService, 1000);

      // Assert
      assertFalse(result, "Should return false when secret key is empty");
    }

    @Test
    @DisplayName("Should return false when access key is whitespace only")
    public void testWhitespaceAccessKey() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("   ");
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); // pragma: allowlist secret

      // Act
      boolean result =
          PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService, 1000);

      // Assert
      assertFalse(result, "Should return false when access key is whitespace only");
    }

    @Test
    @DisplayName("Should return false when getAWSCredentials throws exception")
    public void testGetCredentialsException() {
      // Arrange
      when(mockAttestationService.getAWSCredentials())
          .thenThrow(new RuntimeException("Credentials error"));

      // Act
      boolean result =
          PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService, 1000);

      // Assert
      assertFalse(result, "Should return false when getAWSCredentials throws exception");
    }

    @Test
    @DisplayName("Should return false when region is null")
    public void testNullRegion() {
      // Arrange
      BasicAWSCredentials basicCredentials =
          new BasicAWSCredentials(
              // pragma: allowlist nextline secret
              "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
      when(mockAttestationService.getAWSCredentials()).thenReturn(basicCredentials);
      when(mockAttestationService.getAWSRegion()).thenReturn(null);

      // Act
      boolean result =
          PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService, 1000);

      // Assert
      assertFalse(result, "Should return false when region is null");
    }

    @Test
    @DisplayName("Should return false when region is empty")
    public void testEmptyRegion() {
      // Arrange
      BasicAWSCredentials basicCredentials =
          new BasicAWSCredentials(
              // pragma: allowlist nextline secret
              "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
      when(mockAttestationService.getAWSCredentials()).thenReturn(basicCredentials);
      when(mockAttestationService.getAWSRegion()).thenReturn("");

      // Act
      boolean result =
          PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService, 1000);

      // Assert
      assertFalse(result, "Should return false when region is empty");
    }
  }

  @Nested
  @DisplayName("isValidArnForWif Tests")
  class IsValidArnForWifTests {

    @Test
    @DisplayName("Should return true for valid IAM user ARN")
    public void testValidIamUserArn() {
      assertTrue(
          PlatformDetectionUtil.isValidArnForWif("arn:aws:iam::123456789012:user/testuser"),
          "Should accept IAM user ARN");
      assertTrue(
          PlatformDetectionUtil.isValidArnForWif("arn:aws:iam::123456789012:user/path/to/user"),
          "Should accept IAM user ARN with path");
      assertTrue(
          PlatformDetectionUtil.isValidArnForWif("arn:aws-cn:iam::123456789012:user/testuser"),
          "Should accept IAM user ARN in China partition");
    }

    @Test
    @DisplayName("Should return true for valid assumed role ARN")
    public void testValidAssumedRoleArn() {
      assertTrue(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:sts::123456789012:assumed-role/role-name/session-name"),
          "Should accept assumed role ARN");
      assertTrue(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:sts::123456789012:assumed-role/path/to/role/session"),
          "Should accept assumed role ARN with path");
      assertTrue(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws-cn:sts::123456789012:assumed-role/role-name/session-name"),
          "Should accept assumed role ARN in China partition");
    }

    @Test
    @DisplayName("Should return false for invalid ARN patterns")
    public void testInvalidArnPatterns() {
      assertFalse(PlatformDetectionUtil.isValidArnForWif(null), "Should reject null ARN");
      assertFalse(PlatformDetectionUtil.isValidArnForWif(""), "Should reject empty ARN");
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif("   "), "Should reject whitespace-only ARN");
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif("not-an-arn"), "Should reject non-ARN string");
    }

    @Test
    @DisplayName("Should return false for non-WIF ARN types")
    public void testNonWifArnTypes() {
      // IAM role (not assumed-role)
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif("arn:aws:iam::123456789012:role/test-role"),
          "Should reject IAM role ARN (not assumed-role)");

      // IAM group
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif("arn:aws:iam::123456789012:group/test-group"),
          "Should reject IAM group ARN");

      // S3 bucket
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif("arn:aws:s3:::my-bucket"),
          "Should reject S3 bucket ARN");

      // EC2 instance
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0"),
          "Should reject EC2 instance ARN");

      // Lambda function
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:lambda:us-east-1:123456789012:function:my-function"),
          "Should reject Lambda function ARN");
    }

    @Test
    @DisplayName("Should return false for malformed IAM user ARNs")
    public void testMalformedIamUserArns() {
      // Missing user name
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif("arn:aws:iam::123456789012:user/"),
          "Should reject IAM user ARN without username");

      // Missing account ID
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif("arn:aws:iam:::user/testuser"),
          "Should reject IAM user ARN without account ID");
    }

    @Test
    @DisplayName("Should return false for malformed assumed role ARNs")
    public void testMalformedAssumedRoleArns() {
      // Missing everything after assumed-role/
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif("arn:aws:sts::123456789012:assumed-role/"),
          "Should reject assumed role ARN with nothing after assumed-role/");

      // Missing account ID
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif("arn:aws:sts:::assumed-role/role-name/session"),
          "Should reject assumed role ARN without account ID");
    }
  }
}
