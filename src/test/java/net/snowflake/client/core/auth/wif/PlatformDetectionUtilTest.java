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
    @DisplayName("Should return true for valid credentials")
    public void testValidCredentials() {
      // Arrange
      when(mockAttestationService.getAWSCredentials()).thenReturn(mockCredentials);
      when(mockCredentials.getAWSAccessKeyId()).thenReturn("AKIAIOSFODNN7EXAMPLE");
      when(mockCredentials.getAWSSecretKey())
          .thenReturn("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

      // Act
      boolean result = PlatformDetectionUtil.hasValidAwsIdentityForWif(mockAttestationService);

      // Assert
      assertTrue(result, "Should return true for valid credentials");
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
    @DisplayName("Should work with BasicAWSCredentials implementation")
    public void testWithBasicAWSCredentials() {
      // Arrange
      BasicAWSCredentials basicCredentials =
          new BasicAWSCredentials(
              "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
      when(mockAttestationService.getAWSCredentials()).thenReturn(basicCredentials);

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
}
