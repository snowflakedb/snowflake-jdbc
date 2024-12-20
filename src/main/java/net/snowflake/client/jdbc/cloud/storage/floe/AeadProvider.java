package net.snowflake.client.jdbc.cloud.storage.floe;

import java.security.GeneralSecurityException;
import javax.crypto.SecretKey;

public interface AeadProvider {
  byte[] encrypt(SecretKey key, byte[] iv, byte[] aad, byte[] plaintext)
      throws GeneralSecurityException;

  byte[] decrypt(SecretKey key, byte[] iv, byte[] aad, byte[] ciphertext)
      throws GeneralSecurityException;
}
