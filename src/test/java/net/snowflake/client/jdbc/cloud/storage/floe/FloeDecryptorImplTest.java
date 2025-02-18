package net.snowflake.client.jdbc.cloud.storage.floe;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import javax.crypto.AEADBadTagException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

class FloeDecryptorImplTest {
  private final SecretKey secretKey = new SecretKeySpec(new byte[32], "AES");
  private final byte[] aad = "Test AAD".getBytes(StandardCharsets.UTF_8);

  @Test
  void shouldDecryptCiphertext() throws Exception {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
        FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader())) {
      byte[] firstSegment = encryptor.processSegment(new byte[8]);
      byte[] lastSegment = encryptor.processLastSegment(new byte[4]);

      assertArrayEquals(new byte[8], decryptor.processSegment(firstSegment));
      assertArrayEquals(new byte[4], decryptor.processSegment(lastSegment));
    }
  }

  @Test
  void shouldDecryptLastSegmentZeroLength() throws Exception {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
        FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader())) {
      byte[] lastSegment = encryptor.processLastSegment(new byte[0]);
      assertArrayEquals(new byte[0], decryptor.processSegment(lastSegment));
    }
  }

  @Test
  void shouldDecryptLastSegmentFullLength() throws Exception {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
        FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader())) {
      byte[] lastSegment = encryptor.processLastSegment(new byte[8]);
      assertArrayEquals(new byte[8], decryptor.processSegment(lastSegment));
    }
  }

  @Test
  void shouldThrowExceptionIfSegmentLengthIsMismatched() throws Exception {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
        FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader())) {
      byte[] ciphertext = encryptor.processSegment(new byte[8]);
      byte[] prunedCiphertext = new byte[12];
      ByteBuffer.wrap(ciphertext).get(prunedCiphertext);
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class, () -> decryptor.processSegment(prunedCiphertext));
      assertEquals("segment length mismatch, expected 40, got 12", e.getMessage());
      byte[] extendedCiphertext = new byte[1024];
      ByteBuffer.wrap(extendedCiphertext).put(ciphertext);
      e =
          assertThrows(
              IllegalArgumentException.class, () -> decryptor.processSegment(extendedCiphertext));
      assertEquals("segment length mismatch, expected 40, got 1024", e.getMessage());
      encryptor.processLastSegment(new byte[4]);
    }
  }

  @Test
  void shouldThrowExceptionIfLastSegmentLengthIsMismatched() throws Exception {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
        FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader())) {
      encryptor.processLastSegment(new byte[4]);
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class, () -> decryptor.processSegment(new byte[12]));
      assertEquals("last segment is too short", e.getMessage());
      e =
          assertThrows(
              IllegalArgumentException.class, () -> decryptor.processSegment(new byte[1024]));
      assertEquals("last segment is too long", e.getMessage());
    }
  }

  @Test
  void shouldThrowExceptionIfLastSegmentLengthMarkerIsNotMinusOne() throws Exception {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
        FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader())) {
      encryptor.processLastSegment(new byte[4]);
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class, () -> decryptor.processSegment(new byte[40]));
      assertEquals("last segment length marker mismatch, expected: 40, got: 0", e.getMessage());
    }
  }

  @Test
  void shouldThrowExceptionIfSegmentIsTampered() throws Exception {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
        FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader())) {
      byte[] ciphertext = encryptor.processLastSegment(new byte[8]);
      ciphertext[39]++;
      RuntimeException e =
          assertThrows(RuntimeException.class, () -> decryptor.processSegment(ciphertext));
      assertEquals(e.getCause().getClass(), AEADBadTagException.class);
    }
  }

  @Test
  void shouldThrowExceptionIfSegmentAreOutOfOrder() throws Exception {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    try (FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
        FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader())) {
      byte[] ciphertext1 = encryptor.processSegment(new byte[8]);
      byte[] ciphertext2 = encryptor.processSegment(new byte[8]);
      encryptor.processLastSegment(new byte[4]);
      RuntimeException e =
          assertThrows(RuntimeException.class, () -> decryptor.processSegment(ciphertext2));
      assertEquals(e.getCause().getClass(), AEADBadTagException.class);
    }
  }
}
