package net.snowflake.client.internal.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import org.bouncycastle.util.encoders.Base64;
import org.junit.jupiter.api.Test;

// Values generated on https://gchq.github.io/CyberChef/#recipe=AES_Encrypt
public class GcmEncryptionProviderTest {
  private final SecureRandom random = new SecureRandom();
  private final byte[] encryptionKeyBytes =
      Base64.decode("FzJiKSkfEyiGSANo//gRgDJ0UjMj3r4aDNvDPLhDEls="); // pragma: allowlist secret
  private final byte[] iv = Base64.decode("zszvjmQBz+xF/jN5b8S0Fg==");
  private final byte[] plaintext =
      "The quick brown fox jumps over the lazy dog!".getBytes(StandardCharsets.UTF_8);

  @Test
  void testEncryptionAndDecryptionWithoutAad() throws Exception {
    byte[] expectedCiphertext =
        Base64.decode(
            "Gk0YzYhaJz9AxmR3ZSslMlHErHWzgxRmI/4jg9Oz2oyjS+iIn4/cgtjNPtlAAoGgbr4S9xlD1lyJCg32"); // pragma: allowlist secret

    byte[] ciphertext = GcmEncryptionProvider.encrypt(encryptionKeyBytes, plaintext, iv, null);
    assertArrayEquals(expectedCiphertext, ciphertext);

    byte[] decryptedPlaintext =
        GcmEncryptionProvider.decrypt(encryptionKeyBytes, ciphertext, iv, null);
    assertArrayEquals(decryptedPlaintext, plaintext);
  }

  @Test
  void testEncryptionAndDecryptionWithZeroLengthAad() throws Exception {
    byte[] expectedCiphertext =
        Base64.decode(
            "Gk0YzYhaJz9AxmR3ZSslMlHErHWzgxRmI/4jg9Oz2oyjS+iIn4/cgtjNPtlAAoGgbr4S9xlD1lyJCg32"); // pragma: allowlist secret

    byte[] ciphertext =
        GcmEncryptionProvider.encrypt(encryptionKeyBytes, plaintext, iv, new byte[0]);
    assertArrayEquals(expectedCiphertext, ciphertext);

    byte[] decryptedPlaintext =
        GcmEncryptionProvider.decrypt(encryptionKeyBytes, ciphertext, iv, new byte[0]);
    assertArrayEquals(decryptedPlaintext, plaintext);
  }

  @Test
  void testEncryptionAndDecryptionWithAad() throws Exception {
    byte[] aad = "test AAD".getBytes(StandardCharsets.UTF_8);
    byte[] expectedCiphertext =
        Base64.decode(
            "Gk0YzYhaJz9AxmR3ZSslMlHErHWzgxRmI/4jg9Oz2oyjS+iIn4/cgtjNPtmzJOwDo0JJ/dM12E5iDRVD"); // pragma: allowlist secret

    byte[] ciphertext = GcmEncryptionProvider.encrypt(encryptionKeyBytes, plaintext, iv, aad);
    assertArrayEquals(expectedCiphertext, ciphertext);

    byte[] decryptedPlaintext =
        GcmEncryptionProvider.decrypt(encryptionKeyBytes, ciphertext, iv, aad);
    assertArrayEquals(decryptedPlaintext, plaintext);
  }
}
