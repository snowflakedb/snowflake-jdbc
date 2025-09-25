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
    @DisplayName("Should return true for valid user ARN with valid credentials")
    public void testValiCredentialsAndUserArn() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("AKIAIOSFODNN7EXAMPLE");
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
      when(mockAttestationService.getArn())
          .thenReturn("arn:aws:iam::123456789012:user/ExampleUser");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertTrue(result, "Should return true for valid user ARN with valid credentials");
    }

    @Test
    @DisplayName("Should return true for valid assumed role ARN with valid credentials")
    public void testValidCredentialsAndAssumedRoleArn() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("AKIAIOSFODNN7EXAMPLE");
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
      when(mockAttestationService.getArn())
          .thenReturn("arn:aws:sts::123456789012:assumed-role/MyRole/MySession");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertTrue(result, "Should return true for valid assumed role ARN with valid credentials");
    }

    @Test
    @DisplayName("Should return false when credentials are null")
    public void testNullCredentials() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(null);

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

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
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertFalse(result, "Should return false when access key is null");
    }

    @Test
    @DisplayName("Should return false when secret key is null")
    public void testNullSecretKey() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("AKIAIOSFODNN7EXAMPLE");
      when(mockCredentials.getAWSSecretKey()).thenReturn(null);

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

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
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertFalse(result, "Should return false when access key is empty");
    }

    @Test
    @DisplayName("Should return false when secret key is empty")
    public void testEmptySecretKey() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("AKIAIOSFODNN7EXAMPLE");
      when(mockCredentials.getAWSSecretKey()).thenReturn("");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

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
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertFalse(result, "Should return false when access key is whitespace only");
    }

    @Test
    @DisplayName("Should return false when ARN is null")
    public void testNullArn() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("AKIAIOSFODNN7EXAMPLE");
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
      when(mockAttestationService.getArn()).thenReturn(null);

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertFalse(result, "Should return false when ARN is null");
    }

    @Test
    @DisplayName("Should return false when ARN is empty")
    public void testEmptyArn() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("AKIAIOSFODNN7EXAMPLE");
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
      when(mockAttestationService.getArn()).thenReturn("");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertFalse(result, "Should return false when ARN is empty");
    }

    @Test
    @DisplayName("Should return false when ARN is whitespace only")
    public void testWhitespaceArn() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("AKIAIOSFODNN7EXAMPLE");
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
      when(mockAttestationService.getArn()).thenReturn("   ");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertFalse(result, "Should return false when ARN is whitespace only");
    }

    @Test
    @DisplayName("Should return false for invalid ARN format")
    public void testInvalidArnFormat() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("AKIAIOSFODNN7EXAMPLE");
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
      when(mockAttestationService.getArn()).thenReturn("invalid-arn-format");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertFalse(result, "Should return false for invalid ARN format");
    }

    @Test
    @DisplayName("Should return false for unsupported ARN type (EC2 instance)")
    public void testUnsupportedEc2InstanceArn() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("AKIAIOSFODNN7EXAMPLE");
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
      when(mockAttestationService.getArn())
          .thenReturn("arn:aws:ec2::123456789012:instance/i-1234567890abcdef0");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertFalse(result, "Should return false for unsupported EC2 instance ARN");
    }

    @Test
    @DisplayName("Should return false for unsupported ARN type (S3 bucket)")
    public void testUnsupportedS3BucketArn() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("AKIAIOSFODNN7EXAMPLE");
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
      when(mockAttestationService.getArn()).thenReturn("arn:aws:s3:::my-bucket");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertFalse(result, "Should return false for unsupported S3 bucket ARN");
    }

    @Test
    @DisplayName("Should return false when getAWSCredentials throws exception")
    public void testGetCredentialsException() {
      // Arrange
      when(mockAttestationService.getAWSCredentials())
          .thenThrow(new RuntimeException("Credentials error"));

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertFalse(result, "Should return false when getAWSCredentials throws exception");
    }

    @Test
    @DisplayName("Should return false when getArn throws exception")
    public void testGetArnException() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("AKIAIOSFODNN7EXAMPLE");
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
      when(mockAttestationService.getArn()).thenThrow(new RuntimeException("ARN error"));

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertFalse(result, "Should return false when getArn throws exception");
    }

    @Test
    @DisplayName("Should work with BasicAWSCredentials implementation")
    public void testWithBasicAWSCredentials() {
      // Arrange
      BasicAWSCredentials basicCredentials =
          new BasicAWSCredentials(
              "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
      when(mockAttestationService.getAWSCredentials()).thenReturn(basicCredentials);
      when(mockAttestationService.getArn()).thenReturn("arn:aws:iam::123456789012:user/TestUser");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertTrue(result, "Should work with BasicAWSCredentials implementation");
    }
  }

  @Nested
  @DisplayName("hasValidAwsCredentials Tests")
  class HasValidAwsCredentialsTests {

    @Test
    @DisplayName("Should return true for valid credentials via attestation service")
    public void testValidCredentialsViaAttestationService() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("AKIAIOSFODNN7EXAMPLE");
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsCredentials(mockAttestationService);

      // Assert
      assertTrue(result, "Should return true for valid credentials via attestation service");
    }

    @Test
    @DisplayName("Should return false when attestation service throws exception")
    public void testAttestationServiceException() {
      // Arrange
      when(mockAttestationService.getAWSCredentials())
          .thenThrow(new RuntimeException("Service error"));

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsCredentials(mockAttestationService);

      // Assert
      assertFalse(result, "Should return false when attestation service throws exception");
    }

    @Test
    @DisplayName("Should return true for valid BasicAWSCredentials")
    public void testValidBasicCredentials() {
      // Arrange
      BasicAWSCredentials credentials =
          new BasicAWSCredentials(
              "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsCredentials(credentials);

      // Assert
      assertTrue(result, "Should return true for valid BasicAWSCredentials");
    }

    @Test
    @DisplayName("Should return false for null credentials")
    public void testNullCredentialsDirect() {
      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsCredentials((AWSCredentials) null);

      // Assert
      assertFalse(result, "Should return false for null credentials");
    }
  }

  @Nested
  @DisplayName("isValidArnForWif Tests")
  class IsValidArnForWifTests {

    @Test
    @DisplayName("Should return true for valid IAM user ARN")
    public void testValidIamUserArn() {
      // Act & Assert
      assertTrue(
          PlatformDetectionUtil.isValidArnForWif("arn:aws:iam::123456789012:user/ExampleUser"));
      assertTrue(
          PlatformDetectionUtil.isValidArnForWif("arn:aws:iam::123456789012:user/path/to/user"));
      assertTrue(
          PlatformDetectionUtil.isValidArnForWif("arn:aws-us-gov:iam::123456789012:user/GovUser"));
    }

    @Test
    @DisplayName("Should return true for valid STS assumed role ARN")
    public void testValidStsAssumedRoleArn() {
      // Act & Assert
      assertTrue(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:sts::123456789012:assumed-role/MyRole/MySession"));
      assertTrue(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:sts::123456789012:assumed-role/path/to/role/session"));
      assertTrue(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws-us-gov:sts::123456789012:assumed-role/GovRole/GovSession"));
    }

    @Test
    @DisplayName("Should return false for invalid ARN formats")
    public void testInvalidArnFormats() {
      // Act & Assert
      assertFalse(PlatformDetectionUtil.isValidArnForWif(null));
      assertFalse(PlatformDetectionUtil.isValidArnForWif(""));
      assertFalse(PlatformDetectionUtil.isValidArnForWif("   "));
      assertFalse(PlatformDetectionUtil.isValidArnForWif("invalid-arn"));
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:ec2::123456789012:instance/i-1234567890abcdef0"));
      assertFalse(PlatformDetectionUtil.isValidArnForWif("arn:aws:s3:::my-bucket"));
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:lambda:us-east-1:123456789012:function:my-function"));
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:iam::123456789012:role/MyRole")); // role, not assumed-role
    }

    @Test
    @DisplayName("Should return false for malformed user ARNs")
    public void testMalformedUserArns() {
      // Act & Assert
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:iam::123456789012:user/")); // empty user name
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:iam::123456789012:user")); // missing user name
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:iam:123456789012:user/MyUser")); // missing region separator
    }

    @Test
    @DisplayName("Should return false for malformed assumed role ARNs")
    public void testMalformedAssumedRoleArns() {
      // Act & Assert
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:sts::123456789012:assumed-role/")); // empty role/session
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:sts::123456789012:assumed-role")); // missing role/session
      assertFalse(
          PlatformDetectionUtil.isValidArnForWif(
              "arn:aws:sts:123456789012:assumed-role/MyRole/MySession")); // missing region
      // separator
    }
  }
}
