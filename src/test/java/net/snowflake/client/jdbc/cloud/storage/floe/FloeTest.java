package net.snowflake.client.jdbc.cloud.storage.floe;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class FloeTest {
  byte[] aad = "This is AAD".getBytes(StandardCharsets.UTF_8);
  SecretKey secretKey = new SecretKeySpec(new byte[32], "FLOE");

  @Nested
  class HeaderTests {
    @Test
    void validateHeaderMatchesForEncryptionAndDecryption() {
      FloeParameterSpec parameterSpec =
          new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 1024, 4);
      Floe floe = Floe.getInstance(parameterSpec);

      FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
      byte[] header = encryptor.getHeader();
      assertDoesNotThrow(() -> floe.createDecryptor(secretKey, aad, header));
    }

    @Test
    void validateHeaderDoesNotMatchInParams() {
      FloeParameterSpec parameterSpec =
          new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 1024, 4);
      Floe floe = Floe.getInstance(parameterSpec);
      FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
      byte[] header = encryptor.getHeader();
      header[0] = 12;
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class, () -> floe.createDecryptor(secretKey, aad, header));
      assertEquals(e.getMessage(), "invalid parameters header");
    }

    @Test
    void validateHeaderDoesNotMatchInIV() {
      FloeParameterSpec parameterSpec =
          new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 1024, 4);
      Floe floe = Floe.getInstance(parameterSpec);
      FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
      byte[] header = encryptor.getHeader();
      header[11]++;
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class, () -> floe.createDecryptor(secretKey, aad, header));
      assertEquals(e.getMessage(), "invalid header tag");
    }

    @Test
    void validateHeaderDoesNotMatchInHeaderTag() {
      FloeParameterSpec parameterSpec =
          new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 4096, 4);
      Floe floe = Floe.getInstance(parameterSpec);
      FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
      byte[] header = encryptor.getHeader();
      header[header.length - 3]++;
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class, () -> floe.createDecryptor(secretKey, aad, header));
      assertEquals(e.getMessage(), "invalid header tag");
    }
  }

  @Test
  void testSegmentEncryptedAndDecrypted() {
    FloeParameterSpec parameterSpec =
        new FloeParameterSpec(
            Aead.AES_GCM_256,
            Hash.SHA384,
            40,
            new FloeIvLength(32),
            new IncrementingFloeRandom(),
            4);
    Floe floe = Floe.getInstance(parameterSpec);
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    byte[] header = encryptor.getHeader();
    FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, header);
    byte[] testData = new byte[8];
    byte[] ciphertext = encryptor.processSegment(testData);
    byte[] result = decryptor.processSegment(ciphertext);
    assertArrayEquals(testData, result);
  }

  @Test
  void testSegmentEncryptedAndDecryptedWithRandomData() {
    FloeParameterSpec parameterSpec =
        new FloeParameterSpec(
            Aead.AES_GCM_256,
            Hash.SHA384,
            40,
            new FloeIvLength(32),
            new IncrementingFloeRandom(),
            4);
    Floe floe = Floe.getInstance(parameterSpec);
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    byte[] header = encryptor.getHeader();
    FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, header);
    byte[] testData = new byte[8];
    new SecureRandom().nextBytes(testData);
    byte[] ciphertext = encryptor.processSegment(testData);
    byte[] result = decryptor.processSegment(ciphertext);
    assertArrayEquals(testData, result);
  }

  @Test
  void testSegmentEncryptedAndDecryptedWithDerivedKeyRotation() {
    FloeParameterSpec parameterSpec =
        new FloeParameterSpec(
            Aead.AES_GCM_256,
            Hash.SHA384,
            40,
            new FloeIvLength(32),
            new IncrementingFloeRandom(),
            4);
    Floe floe = Floe.getInstance(parameterSpec);
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    byte[] header = encryptor.getHeader();
    FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, header);
    byte[] testData = new byte[8];
    for (int i = 0; i < 10; i++) {
      byte[] ciphertext = encryptor.processSegment(testData);
      byte[] result = decryptor.processSegment(ciphertext);
      assertArrayEquals(testData, result);
    }
  }
}
