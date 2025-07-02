package net.snowflake.client.jdbc.cloud.storage;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import net.snowflake.client.jdbc.MatDesc;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;

class GcmEncryptionProvider {
  private static final int TAG_LENGTH_IN_BITS = 128;
  private static final int IV_LENGTH_IN_BYTES = 12;
  private static final String AES = "AES";
  private static final String FILE_CIPHER = "AES/GCM/NoPadding";
  private static final String KEY_CIPHER = "AES/GCM/NoPadding";
  private static final int BUFFER_SIZE = 8 * 1024 * 1024; // 2 MB
  private static final ThreadLocal<SecureRandom> random =
      new ThreadLocal<>().withInitial(SecureRandom::new);
  private static final Base64.Decoder base64Decoder = Base64.getDecoder();

  static InputStream encrypt(
      StorageObjectMetadata meta,
      long originalContentLength,
      InputStream src,
      RemoteStoreFileEncryptionMaterial encMat,
      SnowflakeStorageClient client,
      byte[] dataAad,
      byte[] keyAad)
      throws InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException,
          BadPaddingException, NoSuchPaddingException, NoSuchAlgorithmException {

    byte[] kek = base64Decoder.decode(encMat.getQueryStageMasterKey());
    int keySize = kek.length;
    byte[] keyBytes = new byte[keySize];
    byte[] dataIvBytes = new byte[IV_LENGTH_IN_BYTES];
    byte[] keyIvBytes = new byte[IV_LENGTH_IN_BYTES];
    initRandomIvsAndFileKey(dataIvBytes, keyIvBytes, keyBytes);
    byte[] encryptedKey = encryptKey(kek, keyBytes, keyIvBytes, keyAad);
    CipherInputStream cis = encryptContent(src, keyBytes, dataIvBytes, dataAad);
    addEncryptionMetadataToStorageClient(
        meta,
        originalContentLength,
        encMat,
        client,
        keySize,
        encryptedKey,
        dataIvBytes,
        keyIvBytes,
        keyAad,
        dataAad);
    return cis;
  }

  private static void initRandomIvsAndFileKey(
      byte[] dataIvData, byte[] fileKeyIvData, byte[] fileKeyBytes) {
    random.get().nextBytes(dataIvData);
    random.get().nextBytes(fileKeyIvData);
    random.get().nextBytes(fileKeyBytes);
  }

  private static byte[] encryptKey(byte[] kekBytes, byte[] keyBytes, byte[] keyIvData, byte[] aad)
      throws InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException,
          BadPaddingException, NoSuchPaddingException, NoSuchAlgorithmException {
    SecretKey kek = new SecretKeySpec(kekBytes, 0, kekBytes.length, AES);
    GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(TAG_LENGTH_IN_BITS, keyIvData);
    Cipher keyCipher = Cipher.getInstance(KEY_CIPHER);
    keyCipher.init(Cipher.ENCRYPT_MODE, kek, gcmParameterSpec);
    if (aad != null) {
      keyCipher.updateAAD(aad);
    }
    return keyCipher.doFinal(keyBytes);
  }

  private static CipherInputStream encryptContent(
      InputStream src, byte[] keyBytes, byte[] dataIvBytes, byte[] aad)
      throws InvalidKeyException, InvalidAlgorithmParameterException, NoSuchPaddingException,
          NoSuchAlgorithmException {
    SecretKey fileKey = new SecretKeySpec(keyBytes, 0, keyBytes.length, AES);
    GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(TAG_LENGTH_IN_BITS, dataIvBytes);
    Cipher fileCipher = Cipher.getInstance(FILE_CIPHER);
    fileCipher.init(Cipher.ENCRYPT_MODE, fileKey, gcmParameterSpec);
    if (aad != null) {
      fileCipher.updateAAD(aad);
    }
    return new CipherInputStream(src, fileCipher);
  }

  private static void addEncryptionMetadataToStorageClient(
      StorageObjectMetadata meta,
      long contentLength,
      RemoteStoreFileEncryptionMaterial encMat,
      SnowflakeStorageClient client,
      int keySize,
      byte[] encryptedKey,
      byte[] dataIvData,
      byte[] keyIvData,
      byte[] keyAad,
      byte[] dataAad) {
    MatDesc matDesc = new MatDesc(encMat.getSmkId(), encMat.getQueryId(), keySize * 8);
    client.addEncryptionMetadataForGcm(
        meta, matDesc, encryptedKey, dataIvData, keyIvData, keyAad, dataAad, contentLength);
  }

  static void decryptFile(
      File file,
      String encryptedFileKeyBase64,
      String dataIvBase64,
      String keyIvBase64,
      RemoteStoreFileEncryptionMaterial encMat,
      String dataAadBase64,
      String keyAadBase64)
      throws InvalidKeyException, IllegalBlockSizeException, BadPaddingException,
          InvalidAlgorithmParameterException, IOException, NoSuchPaddingException,
          NoSuchAlgorithmException {
    byte[] encryptedKeyBytes = base64Decoder.decode(encryptedFileKeyBase64);
    byte[] dataIvBytes = base64Decoder.decode(dataIvBase64);
    byte[] keyIvBytes = base64Decoder.decode(keyIvBase64);
    byte[] kekBytes = base64Decoder.decode(encMat.getQueryStageMasterKey());
    byte[] keyAad = base64Decoder.decode(keyAadBase64);
    byte[] dataAad = base64Decoder.decode(dataAadBase64);

    byte[] keyBytes = decryptKey(kekBytes, keyIvBytes, encryptedKeyBytes, keyAad);
    decryptContentFromFile(file, keyBytes, dataIvBytes, dataAad);
  }

  static InputStream decryptStream(
      InputStream inputStream,
      String encryptedKeyBase64,
      String dataIvBase64,
      String keyIvBase64,
      RemoteStoreFileEncryptionMaterial encMat,
      String dataAad,
      String keyAad)
      throws InvalidKeyException, BadPaddingException, IllegalBlockSizeException,
          InvalidAlgorithmParameterException, NoSuchPaddingException, NoSuchAlgorithmException {
    byte[] encryptedKeyBytes = base64Decoder.decode(encryptedKeyBase64);
    byte[] ivBytes = base64Decoder.decode(dataIvBase64);
    byte[] kekIvBytes = base64Decoder.decode(keyIvBase64);
    byte[] dataAadBytes = base64Decoder.decode(dataAad);
    byte[] keyAadBytes = base64Decoder.decode(keyAad);
    byte[] kekBytes = base64Decoder.decode(encMat.getQueryStageMasterKey());

    byte[] fileKeyBytes = decryptKey(kekBytes, kekIvBytes, encryptedKeyBytes, keyAadBytes);
    return decryptContentFromStream(inputStream, ivBytes, fileKeyBytes, dataAadBytes);
  }

  private static CipherInputStream decryptContentFromStream(
      InputStream inputStream, byte[] ivBytes, byte[] fileKeyBytes, byte[] aad)
      throws InvalidKeyException, InvalidAlgorithmParameterException, NoSuchPaddingException,
          NoSuchAlgorithmException {
    GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(TAG_LENGTH_IN_BITS, ivBytes);
    SecretKey fileKey = new SecretKeySpec(fileKeyBytes, AES);
    Cipher fileCipher = Cipher.getInstance(FILE_CIPHER);
    fileCipher.init(Cipher.DECRYPT_MODE, fileKey, gcmParameterSpec);
    if (aad != null) {
      fileCipher.updateAAD(aad);
    }
    return new CipherInputStream(inputStream, fileCipher);
  }

  private static void decryptContentFromFile(
      File file, byte[] fileKeyBytes, byte[] cekIvBytes, byte[] aad)
      throws InvalidKeyException, InvalidAlgorithmParameterException, IOException,
          NoSuchPaddingException, NoSuchAlgorithmException {
    SecretKey fileKey = new SecretKeySpec(fileKeyBytes, AES);
    GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(TAG_LENGTH_IN_BITS, cekIvBytes);
    byte[] buffer = new byte[BUFFER_SIZE];
    Cipher fileCipher = Cipher.getInstance(FILE_CIPHER);
    fileCipher.init(Cipher.DECRYPT_MODE, fileKey, gcmParameterSpec);
    if (aad != null) {
      fileCipher.updateAAD(aad);
    }

    long totalBytesRead = 0;
    try (InputStream is = Files.newInputStream(file.toPath(), READ);
        InputStream cis = new CipherInputStream(is, fileCipher);
        OutputStream os = Files.newOutputStream(file.toPath(), CREATE)) {
      int bytesRead;
      while ((bytesRead = cis.read(buffer)) > -1) {
        os.write(buffer, 0, bytesRead);
        totalBytesRead += bytesRead;
      }
    }

    try (FileOutputStream fos = new FileOutputStream(file, true);
        FileChannel fc = fos.getChannel()) {
      fc.truncate(totalBytesRead);
    }
  }

  private static byte[] decryptKey(byte[] kekBytes, byte[] ivBytes, byte[] keyBytes, byte[] aad)
      throws InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException,
          BadPaddingException, NoSuchPaddingException, NoSuchAlgorithmException {
    SecretKey kek = new SecretKeySpec(kekBytes, 0, kekBytes.length, AES);
    GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(TAG_LENGTH_IN_BITS, ivBytes);
    Cipher keyCipher = Cipher.getInstance(KEY_CIPHER);
    keyCipher.init(Cipher.DECRYPT_MODE, kek, gcmParameterSpec);
    if (aad != null) {
      keyCipher.updateAAD(aad);
    }
    return keyCipher.doFinal(keyBytes);
  }
}
