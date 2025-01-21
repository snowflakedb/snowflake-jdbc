package net.snowflake.client.jdbc.cloud.storage.floe;

import org.junit.jupiter.api.Test;

import javax.crypto.AEADBadTagException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FloeDecryptorImplTest {
  private final SecretKey secretKey = new SecretKeySpec(new byte[32], "AES");
  private final byte[] aad = "Test AAD".getBytes(StandardCharsets.UTF_8);

  @Test
  void shouldDecryptCiphertext() {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    byte[] firstSegment = encryptor.processSegment(new byte[8]);
    byte[] lastSegment = encryptor.processLastSegment(new byte[4]);

    FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader());
    assertArrayEquals(new byte[8], decryptor.processSegment(firstSegment));
    assertArrayEquals(new byte[4], decryptor.processLastSegment(lastSegment));
  }

  @Test
  void shouldDecryptLastSegmentZeroLength() {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    byte[] lastSegment = encryptor.processLastSegment(new byte[0]);

    FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader());
    assertArrayEquals(new byte[0], decryptor.processLastSegment(lastSegment));
  }

  @Test
  void shouldDecryptLastSegmentFullLength() {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    byte[] lastSegment = encryptor.processLastSegment(new byte[8]);

    FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader());
    assertArrayEquals(new byte[8], decryptor.processLastSegment(lastSegment));
  }

  @Test
  void shouldThrowExceptionIfSegmentLengthIsMismatched() {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader());
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> decryptor.processSegment(new byte[12]));
    assertEquals("segment length mismatch, expected 40, got 12", e.getMessage());
    e = assertThrows(IllegalArgumentException.class, () -> decryptor.processSegment(new byte[1024]));
    assertEquals("segment length mismatch, expected 40, got 1024", e.getMessage());
  }

  @Test
  void shouldThrowExceptionIfLastSegmentLengthIsMismatched() {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader());
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> decryptor.processLastSegment(new byte[12]));
    assertEquals("last segment is too short", e.getMessage());
    e = assertThrows(IllegalArgumentException.class, () -> decryptor.processLastSegment(new byte[1024]));
    assertEquals("last segment is too long", e.getMessage());
  }

  @Test
  void shouldThrowExceptionIfSegmentLengthInSegmentIsNotMinusOne() {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader());
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> decryptor.processSegment(new byte[40]));
    assertEquals("segment length marker mismatch, expected: -1, got: 0", e.getMessage());
  }

  @Test
  void shouldThrowExceptionIfLastSegmentLengthInSegmentIsNotMinusOne() {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader());
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> decryptor.processLastSegment(new byte[40]));
    assertEquals("last segment length marker mismatch, expected: 40, got: 0", e.getMessage());
  }

  @Test
  void shouldThrowExceptionIfSegmentIsTampered() {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    byte[] ciphertext = encryptor.processSegment(new byte[8]);
    FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader());
    ciphertext[39]++;
    RuntimeException e = assertThrows(RuntimeException.class, () -> decryptor.processSegment(ciphertext));
    assertEquals(e.getCause().getClass(), AEADBadTagException.class);
  }

  @Test
  void shouldThrowExceptionIfSegmentAreOutOfOrder() {
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_256, Hash.SHA384, 40, 32);
    Floe floe = Floe.getInstance(parameterSpec);
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    byte[] ciphertext1 = encryptor.processSegment(new byte[8]);
    byte[] ciphertext2 = encryptor.processSegment(new byte[8]);
    FloeDecryptor decryptor = floe.createDecryptor(secretKey, aad, encryptor.getHeader());
    RuntimeException e = assertThrows(RuntimeException.class, () -> decryptor.processSegment(ciphertext2));
    assertEquals(e.getCause().getClass(), AEADBadTagException.class);
  }
}