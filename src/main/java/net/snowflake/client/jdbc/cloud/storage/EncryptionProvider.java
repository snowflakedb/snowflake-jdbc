/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.cloud.storage;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;

import com.amazonaws.util.Base64;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import net.snowflake.client.jdbc.MatDesc;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;

/**
 * Handles encryption and decryption using AES.
 *
 * @author ppaulus
 */
public class EncryptionProvider {
  private static final String AES = "AES";
  private static final String FILE_CIPHER = "AES/CBC/PKCS5Padding";
  private static final String KEY_CIPHER = "AES/ECB/PKCS5Padding";
  private static final int BUFFER_SIZE = 2 * 1024 * 1024; // 2 MB
  private static SecureRandom secRnd;

  /** Decrypt a InputStream */
  public static InputStream decryptStream(
      InputStream inputStream,
      String keyBase64,
      String ivBase64,
      RemoteStoreFileEncryptionMaterial encMat)
      throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException,
          BadPaddingException, IllegalBlockSizeException, InvalidAlgorithmParameterException {
    byte[] decodedKey = Base64.decode(encMat.getQueryStageMasterKey());

    byte[] keyBytes = Base64.decode(keyBase64);

    byte[] ivBytes = Base64.decode(ivBase64);

    SecretKey queryStageMasterKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, AES);

    Cipher keyCipher = Cipher.getInstance(KEY_CIPHER);

    keyCipher.init(Cipher.DECRYPT_MODE, queryStageMasterKey);

    byte[] fileKeyBytes = keyCipher.doFinal(keyBytes);

    SecretKey fileKey = new SecretKeySpec(fileKeyBytes, 0, decodedKey.length, AES);

    Cipher dataCipher = Cipher.getInstance(FILE_CIPHER);

    IvParameterSpec ivy = new IvParameterSpec(ivBytes);

    dataCipher.init(Cipher.DECRYPT_MODE, fileKey, ivy);

    return new CipherInputStream(inputStream, dataCipher);
  }

  /*
   * decrypt
   * Decrypts a file given the key and iv. Uses AES decryption.
   */
  public static void decrypt(
      File file, String keyBase64, String ivBase64, RemoteStoreFileEncryptionMaterial encMat)
      throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException,
          IllegalBlockSizeException, BadPaddingException, InvalidAlgorithmParameterException,
          IOException {
    byte[] keyBytes = Base64.decode(keyBase64);
    byte[] ivBytes = Base64.decode(ivBase64);
    byte[] qsmkBytes = Base64.decode(encMat.getQueryStageMasterKey());
    final SecretKey fileKey;

    // Decrypt file key
    {
      final Cipher keyCipher = Cipher.getInstance(KEY_CIPHER);
      SecretKey queryStageMasterKey = new SecretKeySpec(qsmkBytes, 0, qsmkBytes.length, AES);
      keyCipher.init(Cipher.DECRYPT_MODE, queryStageMasterKey);
      byte[] fileKeyBytes = keyCipher.doFinal(keyBytes);

      // NB: we assume qsmk.length == fileKey.length
      //     (fileKeyBytes.length may be bigger due to padding)
      fileKey = new SecretKeySpec(fileKeyBytes, 0, qsmkBytes.length, AES);
    }

    // Decrypt file
    {
      final Cipher fileCipher = Cipher.getInstance(FILE_CIPHER);
      final IvParameterSpec iv = new IvParameterSpec(ivBytes);
      final byte[] buffer = new byte[BUFFER_SIZE];
      fileCipher.init(Cipher.DECRYPT_MODE, fileKey, iv);

      long totalBytesRead = 0;
      // Overwrite file contents buffer-wise with decrypted data
      try (InputStream is = Files.newInputStream(file.toPath(), READ);
          InputStream cis = new CipherInputStream(is, fileCipher);
          OutputStream os = Files.newOutputStream(file.toPath(), CREATE); ) {
        int bytesRead;
        while ((bytesRead = cis.read(buffer)) > -1) {
          os.write(buffer, 0, bytesRead);
          totalBytesRead += bytesRead;
        }
      }

      // Discard any padding that the encrypted file had
      try (FileChannel fc = new FileOutputStream(file, true).getChannel()) {
        fc.truncate(totalBytesRead);
      }
    }
  }

  /*
   * encrypt
   * Encrypts a file using AES encryption. The key and iv are generated.
   * The matdesc field is added to the metadata object.
   * The key and iv are added to the JSON block in the encryptionData
   * metadata object.
   */
  public static CipherInputStream encrypt(
      StorageObjectMetadata meta,
      long originalContentLength,
      InputStream src,
      RemoteStoreFileEncryptionMaterial encMat,
      SnowflakeStorageClient client)
      throws InvalidKeyException, InvalidAlgorithmParameterException, NoSuchAlgorithmException,
          NoSuchProviderException, NoSuchPaddingException, FileNotFoundException,
          IllegalBlockSizeException, BadPaddingException {
    final byte[] decodedKey = Base64.decode(encMat.getQueryStageMasterKey());
    final int keySize = decodedKey.length;
    final byte[] fileKeyBytes = new byte[keySize];
    final byte[] ivData;
    final CipherInputStream cis;
    final int blockSz;
    {
      final Cipher fileCipher = Cipher.getInstance(FILE_CIPHER);
      blockSz = fileCipher.getBlockSize();

      // Create IV
      ivData = new byte[blockSz];
      getSecRnd().nextBytes(ivData);
      final IvParameterSpec iv = new IvParameterSpec(ivData);

      // Create file key
      getSecRnd().nextBytes(fileKeyBytes);
      SecretKey fileKey = new SecretKeySpec(fileKeyBytes, 0, keySize, AES);

      // Init cipher
      fileCipher.init(Cipher.ENCRYPT_MODE, fileKey, iv);

      // Create encrypting input stream
      cis = new CipherInputStream(src, fileCipher);
    }

    // Encrypt the file key with the QRMK
    {
      final Cipher keyCipher = Cipher.getInstance(KEY_CIPHER);
      SecretKey queryStageMasterKey = new SecretKeySpec(decodedKey, 0, keySize, AES);

      // Init cipher
      keyCipher.init(Cipher.ENCRYPT_MODE, queryStageMasterKey);
      byte[] encKeK = keyCipher.doFinal(fileKeyBytes);

      // Store metadata
      MatDesc matDesc = new MatDesc(encMat.getSmkId(), encMat.getQueryId(), keySize * 8);
      // Round up length to next multiple of the block size
      // Sizes that are multiples of the block size need to be padded to next
      // multiple
      long contentLength = ((originalContentLength + blockSz) / blockSz) * blockSz;
      client.addEncryptionMetadata(meta, matDesc, ivData, encKeK, contentLength);
    }

    return cis;
  }

  /*
   * getSecRnd
   * Gets a random number for encryption purposes.
   */
  private static synchronized SecureRandom getSecRnd()
      throws NoSuchAlgorithmException, NoSuchProviderException {
    if (secRnd == null) {
      secRnd = SecureRandom.getInstance("SHA1PRNG");
      byte[] bytes = new byte[10];
      secRnd.nextBytes(bytes);
    }
    return secRnd;
  }
}
