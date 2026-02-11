package net.snowflake.client.internal.jdbc.cloud.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import net.snowflake.client.internal.jdbc.MatDesc;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import net.snowflake.floe.Aead;
import net.snowflake.floe.FloeParameterSpec;
import net.snowflake.floe.Hash;
import net.snowflake.floe.stream.FloeDecryptingInputStream;
import net.snowflake.floe.stream.FloeEncryptingInputStream;
import org.apache.commons.io.IOUtils;

class FloeEncryptionProvider {
  private static final int CIPHERTEXT_SEGMENT_LENGTH = 4096;
  private static final int FLOE_IV_LENGTH = 32;
  private static final int GCM_IV_LENGTH = 12;

  private static final ThreadLocal<SecureRandom> secureRandom =
      ThreadLocal.withInitial(SecureRandom::new);

  static InputStream encryptStream(
      StorageObjectMetadata meta,
      long originalContentLength,
      InputStream uploadStream,
      RemoteStoreFileEncryptionMaterial encMat,
      SnowflakeStorageClient storageClient)
      throws InvalidAlgorithmParameterException, NoSuchPaddingException, IllegalBlockSizeException,
          NoSuchAlgorithmException, BadPaddingException, InvalidKeyException {
    byte[] keyIv = new byte[GCM_IV_LENGTH];
    byte[] keyAad = new byte[0]; // TODO
    byte[] contentKey = new byte[Aead.AES_GCM_256.getKeyLength()];
    secureRandom.get().nextBytes(keyIv);
    secureRandom.get().nextBytes(contentKey);
    byte[] floeAad = new byte[0]; // TODO
    SecretKey secretKey = new SecretKeySpec(contentKey, "FLOE");
    MatDesc matDesc =
        new MatDesc(encMat.getSmkId(), encMat.getQueryId(), Aead.AES_GCM_256.getKeyLength());
    byte[] encryptedContentKey =
        GcmEncryptionProvider.encrypt(
            Base64.getDecoder().decode(encMat.getQueryStageMasterKey()), contentKey, keyIv, keyAad);
    FloeParameterSpec floeParameterSpec =
        new FloeParameterSpec(
            Aead.AES_GCM_256, Hash.SHA384, CIPHERTEXT_SEGMENT_LENGTH, FLOE_IV_LENGTH);
    FloeEncryptingInputStream floeStream =
        new FloeEncryptingInputStream(uploadStream, floeParameterSpec, secretKey, floeAad, false);
    storageClient.addEncryptionMetadataForGcm(
        meta,
        matDesc,
        originalContentLength,
        encryptedContentKey,
        keyIv,
        keyAad,
        floeAad,
        floeStream.getHeader());
    return floeStream;
  }

  static InputStream decryptStream(
      InputStream inputStream,
      byte[] key,
      FloeParameterSpec parameterSpec,
      byte[] aad,
      byte[] header)
      throws IOException, InvalidAlgorithmParameterException, IllegalBlockSizeException,
          NoSuchPaddingException, BadPaddingException, NoSuchAlgorithmException,
          InvalidKeyException {
    SecretKey secretKey = new SecretKeySpec(key, "FLOE");
    return new FloeDecryptingInputStream(inputStream, parameterSpec, secretKey, aad, header);
  }

  static void decryptFile(
      File localFile, byte[] key, FloeParameterSpec parameterSpec, byte[] aad, byte[] header)
      throws IOException, InvalidAlgorithmParameterException, IllegalBlockSizeException,
          NoSuchPaddingException, BadPaddingException, NoSuchAlgorithmException,
          InvalidKeyException {
    SecretKey secretKey = new SecretKeySpec(key, "FLOE");
    try (FileInputStream fis = new FileInputStream(localFile);
        FloeDecryptingInputStream decryptingInputStream =
            new FloeDecryptingInputStream(fis, parameterSpec, secretKey, aad, header)) {
      try (FileOutputStream fos = new FileOutputStream(localFile)) {
        IOUtils.copy(decryptingInputStream, fos);
      }
    }
  }
}
