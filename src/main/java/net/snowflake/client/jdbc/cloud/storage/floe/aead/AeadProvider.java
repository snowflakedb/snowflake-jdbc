package net.snowflake.client.jdbc.cloud.storage.floe.aead;

import java.security.GeneralSecurityException;
import javax.crypto.SecretKey;

// Consideration for implementations:
// 1. Implementations does not have to be thread safe, they are used in FLOE in a thread safe manner
// (FLOE encryptor and decryptor creates their own instances).
// 2. Authentication tag is a part of ciphertext:
// a) For encrypt function - auth tag is returned with ciphertext.
// b) For decrypt function - auth tag is passed with ciphertext.
// As long as it isn't strictly required to be at the end of the ciphertext, it is needed to be in
// the correct position for the underlying algorithm.
public interface AeadProvider {
  byte[] encrypt(SecretKey key, byte[] iv, byte[] aad, byte[] plaintext)
      throws GeneralSecurityException;

  byte[] decrypt(SecretKey key, byte[] iv, byte[] aad, byte[] ciphertext)
      throws GeneralSecurityException;
}
