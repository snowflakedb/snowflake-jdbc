package net.snowflake.client.internal.jdbc.cloud.storage;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

class GcmEncryptionProvider {
  private static final int TAG_LENGTH_IN_BITS = 128;
  private static final String AES = "AES";
  private static final String KEY_CIPHER = "AES/GCM/NoPadding";

  static byte[] encrypt(byte[] kekBytes, byte[] keyBytes, byte[] iv, byte[] aad)
      throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException,
          InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
    return process(kekBytes, keyBytes, iv, aad, Cipher.ENCRYPT_MODE);
  }

  static byte[] decrypt(byte[] kekBytes, byte[] keyBytes, byte[] iv, byte[] aad)
      throws InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException,
          BadPaddingException, NoSuchPaddingException, NoSuchAlgorithmException {
    return process(kekBytes, keyBytes, iv, aad, Cipher.DECRYPT_MODE);
  }

  private static byte[] process(
      byte[] kekBytes, byte[] keyBytes, byte[] iv, byte[] aad, int encryptMode)
      throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException,
          InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException {
    SecretKey kek = new SecretKeySpec(kekBytes, 0, kekBytes.length, AES);
    GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(TAG_LENGTH_IN_BITS, iv);
    Cipher keyCipher = Cipher.getInstance(KEY_CIPHER);
    keyCipher.init(encryptMode, kek, gcmParameterSpec);
    if (aad != null) {
      keyCipher.updateAAD(aad);
    }
    return keyCipher.doFinal(keyBytes);
  }
}
