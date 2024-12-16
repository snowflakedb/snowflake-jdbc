package net.snowflake.client.jdbc.cloud.storage.floe;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

class FloeTest {
  @Test
  void validateHeaderMatchesForEncryptionAndDecryption() {
    byte[] aad = "test aad".getBytes(StandardCharsets.UTF_8);
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_128, Hash.SHA384, 1024, 4);
    Floe floe = Floe.getInstance(parameterSpec);
    SecretKey secretKey = new SecretKeySpec(new byte[16], "FLOE");
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    byte[] header = encryptor.getHeader();
    assertDoesNotThrow(() -> floe.createDecryptor(secretKey, aad, header));
  }

  @Test
  void validateHeaderDoesNotMatchInParams() {
    byte[] aad = "test aad".getBytes(StandardCharsets.UTF_8);
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_128, Hash.SHA384, 1024, 4);
    Floe floe = Floe.getInstance(parameterSpec);
    SecretKey secretKey = new SecretKeySpec(new byte[16], "FLOE");
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
    byte[] aad = "test aad".getBytes(StandardCharsets.UTF_8);
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_128, Hash.SHA384, 1024, 4);
    Floe floe = Floe.getInstance(parameterSpec);
    SecretKey secretKey = new SecretKeySpec(new byte[16], "FLOE");
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
    byte[] aad = "test aad".getBytes(StandardCharsets.UTF_8);
    FloeParameterSpec parameterSpec = new FloeParameterSpec(Aead.AES_GCM_128, Hash.SHA384, 1024, 4);
    Floe floe = Floe.getInstance(parameterSpec);
    SecretKey secretKey = new SecretKeySpec(new byte[16], "FLOE");
    FloeEncryptor encryptor = floe.createEncryptor(secretKey, aad);
    byte[] header = encryptor.getHeader();
    header[header.length - 3]++;
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> floe.createDecryptor(secretKey, aad, header));
    assertEquals(e.getMessage(), "invalid header tag");
  }
}
