package net.snowflake.client.jdbc.cloud.storage.floe;

import static com.amazonaws.util.BinaryUtils.toHex;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
    void validateHeaderMatchesForEncryptionAndDecryption() throws Exception {
      FloeParameterSpec parameterSpec =
          new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 1024, 4);
      Floe floe = Floe.getInstance(parameterSpec);

      try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
          FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader())) {
        decryptor.processSegment(encryptor.processLastSegment(new byte[0]));
      }
    }

    @Test
    void validateHeaderDoesNotMatchInParams() throws Exception {
      FloeParameterSpec parameterSpec =
          new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 1024, 4);
      Floe floe = Floe.getInstance(parameterSpec);
      try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad)) {
        byte[] header = encryptor.getHeader();
        header[0] = 12;
        IllegalArgumentException e =
            assertThrows(
                IllegalArgumentException.class, () -> floe.createDecryptor(secretKey, aad, header));
        assertEquals(e.getMessage(), "invalid parameters header");
        encryptor.processLastSegment(new byte[0]);
      }
    }

    @Test
    void validateHeaderDoesNotMatchInIV() throws Exception {
      FloeParameterSpec parameterSpec =
          new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 1024, 4);
      Floe floe = Floe.getInstance(parameterSpec);
      try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad)) {
        byte[] header = encryptor.getHeader();
        header[11]++;
        IllegalArgumentException e =
            assertThrows(
                IllegalArgumentException.class, () -> floe.createDecryptor(secretKey, aad, header));
        assertEquals(e.getMessage(), "invalid header tag");
        encryptor.processLastSegment(new byte[0]);
      }
    }

    @Test
    void validateHeaderDoesNotMatchInHeaderTag() throws Exception {
      FloeParameterSpec parameterSpec =
          new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 4096, 4);
      Floe floe = Floe.getInstance(parameterSpec);
      try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad)) {
        byte[] header = encryptor.getHeader();
        header[header.length - 3]++;
        IllegalArgumentException e =
            assertThrows(
                IllegalArgumentException.class, () -> floe.createDecryptor(secretKey, aad, header));
        assertEquals(e.getMessage(), "invalid header tag");
        encryptor.processLastSegment(new byte[0]);
      }
    }
  }

  @Nested
  class SegmentTests {

    @Test
    void testSegmentEncryptedAndDecrypted() throws Exception {
      FloeParameterSpec parameterSpec =
          new FloeParameterSpec(
              Aead.AES_GCM_256,
              Hash.SHA384,
              40,
              new FloeIvLength(32),
              new IncrementingFloeRandom(678765),
              4,
              1L << 40);
      Floe floe = Floe.getInstance(parameterSpec);
      try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
          FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader())) {
        byte[] testData = new byte[8];
        byte[] ciphertext = encryptor.processLastSegment(testData);
        byte[] result = decryptor.processSegment(ciphertext);
        assertArrayEquals(testData, result);
      }
    }

    @Test
    void testSegmentEncryptedAndDecryptedWithRandomData() throws Exception {
      FloeParameterSpec parameterSpec =
          new FloeParameterSpec(
              Aead.AES_GCM_256,
              Hash.SHA384,
              40,
              new FloeIvLength(32),
              new IncrementingFloeRandom(37665),
              4,
              1L << 40);
      Floe floe = Floe.getInstance(parameterSpec);
      byte[] ciphertext;
      try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
          FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader())) {
        byte[] testData = new byte[8];
        new SecureRandom().nextBytes(testData);
        ciphertext = encryptor.processLastSegment(testData);
        byte[] result = decryptor.processSegment(ciphertext);
        assertArrayEquals(testData, result);
      }
    }

    @Test
    void testSegmentEncryptedAndDecryptedWithDerivedKeyRotation() throws Exception {
      FloeParameterSpec parameterSpec =
          new FloeParameterSpec(
              Aead.AES_GCM_256,
              Hash.SHA384,
              40,
              new FloeIvLength(32),
              new IncrementingFloeRandom(6546),
              4,
              1L << 40);
      Floe floe = Floe.getInstance(parameterSpec);
      try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
          FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader())) {
        byte[] testData = new byte[8];
        for (int i = 0; i < 10; i++) {
          byte[] ciphertext = encryptor.processSegment(testData);
          byte[] result = decryptor.processSegment(ciphertext);
          assertArrayEquals(testData, result);
        }
        byte[] ciphertext = encryptor.processLastSegment(testData);
        decryptor.processSegment(ciphertext);
      }
    }
  }

  @Nested
  class LastSegmentTests {
    @Test
    void testLastSegmentEncryptedAndDecrypted() throws Exception {
      FloeParameterSpec parameterSpec =
          new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 1024, 32);
      Floe floe = Floe.getInstance(parameterSpec);
      try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
          FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader())) {
        byte[] plaintext = new byte[3];
        byte[] encrypted = encryptor.processLastSegment(plaintext);
        byte[] decrypted = decryptor.processSegment(encrypted);
        assertArrayEquals(plaintext, decrypted);
      }
    }

    @Test
    void testDecryptLastSegmentWithReferenceDataWithEmptyLastSegment() throws Exception {
      FloeParameterSpec floeParameterSpec =
          new FloeParameterSpec(
              Aead.AES_GCM_256,
              Hash.SHA384,
              40,
              new FloeIvLength(32),
              new IncrementingFloeRandom(0),
              16,
              1L << 40);
      Floe floe = Floe.getInstance(floeParameterSpec);
      try (FloeEncryptor encryptor =
              floe.createEncryptor(new SecretKeySpec(new byte[16], "FLOE"), new byte[0]);
          FloeDecryptor decryptor =
              floe.createDecryptor(secretKey, new byte[0], encryptor.getHeader())) {
        byte[] plaintext = new byte[8];
        byte[] encryptedFirstSegment = encryptor.processSegment(plaintext);
        byte[] encryptedLastSegment = encryptor.processLastSegment(new byte[0]);

        assertEquals(
            toHex(encryptedFirstSegment),
            "ffffffff0000000100000000000000002dd631464f6a583369b74f546adfa4db9a838732d6338ef4"); // pragma: allowlist secret
        assertEquals(
            toHex(encryptedLastSegment),
            "000000200000000200000000000000004a4082e6b94a8b1b2053f40879402df1"); // pragma:
                                                                                 // allowlist secret

        assertArrayEquals(plaintext, decryptor.processSegment(encryptedFirstSegment));
        assertArrayEquals(new byte[0], decryptor.processSegment(encryptedLastSegment));
      }
    }

    @Test
    void testDecryptLastSegmentWithReferenceDataWithNonEmptyLastSegment() throws Exception {
      FloeParameterSpec floeParameterSpec =
          new FloeParameterSpec(
              Aead.AES_GCM_256,
              Hash.SHA384,
              40,
              new FloeIvLength(32),
              new IncrementingFloeRandom(0),
              16,
              1L << 40);
      Floe floe = Floe.getInstance(floeParameterSpec);
      try (FloeEncryptor encryptor =
              floe.createEncryptor(new SecretKeySpec(new byte[16], "FLOE"), new byte[0]);
          FloeDecryptor decryptor =
              floe.createDecryptor(secretKey, new byte[0], encryptor.getHeader())) {
        byte[] plaintext = new byte[8];
        byte[] encryptedFirstSegment = encryptor.processSegment(plaintext);
        byte[] encryptedLastSegment = encryptor.processLastSegment(plaintext);

        assertEquals(
            toHex(encryptedFirstSegment),
            "ffffffff0000000100000000000000002dd631464f6a583369b74f546adfa4db9a838732d6338ef4"); // pragma: allowlist secret
        assertEquals(
            toHex(encryptedLastSegment),
            "000000280000000200000000000000003b14259ad693c7df7a2d6b9d9912dc70a81205d41ac43a41"); // pragma: allowlist secret

        assertArrayEquals(plaintext, decryptor.processSegment(encryptedFirstSegment));
        assertArrayEquals(plaintext, decryptor.processSegment(encryptedLastSegment));
      }
    }
  }
}
