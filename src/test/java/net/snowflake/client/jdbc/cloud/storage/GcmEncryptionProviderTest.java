package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import net.snowflake.client.jdbc.MatDesc;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class GcmEncryptionProviderTest {
  private final SecureRandom random = new SecureRandom();

  private final ArgumentCaptor<StorageObjectMetadata> storageObjectMetadataArgumentCaptor =
      ArgumentCaptor.forClass(StorageObjectMetadata.class);
  private final ArgumentCaptor<MatDesc> matDescArgumentCaptor =
      ArgumentCaptor.forClass(MatDesc.class);
  private final ArgumentCaptor<byte[]> cekIvDataArgumentCaptor =
      ArgumentCaptor.forClass(byte[].class);
  private final ArgumentCaptor<byte[]> kekIvDataArgumentCaptor =
      ArgumentCaptor.forClass(byte[].class);
  private final ArgumentCaptor<byte[]> encKekArgumentCaptor = ArgumentCaptor.forClass(byte[].class);
  private final ArgumentCaptor<byte[]> keyAadArgumentCaptor = ArgumentCaptor.forClass(byte[].class);
  private final ArgumentCaptor<byte[]> dataAadArgumentCaptor =
      ArgumentCaptor.forClass(byte[].class);
  private final ArgumentCaptor<Long> contentLengthArgumentCaptor =
      ArgumentCaptor.forClass(Long.class);

  private final StorageObjectMetadata meta = mock(StorageObjectMetadata.class);
  private final SnowflakeStorageClient storageClient = mock(SnowflakeStorageClient.class);

  private final String queryStageMasterKey =
      Base64.getEncoder().encodeToString(random.generateSeed(16));
  private final RemoteStoreFileEncryptionMaterial encMat = new RemoteStoreFileEncryptionMaterial();

  byte[] plainText = "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);
  byte[] dataAad = "data aad".getBytes(StandardCharsets.UTF_8);
  byte[] keyAad = "key aad".getBytes(StandardCharsets.UTF_8);

  @Before
  public void setUp() {
    encMat.setQueryStageMasterKey(queryStageMasterKey);
    encMat.setSmkId(123);
    encMat.setQueryId("query-id");
  }

  @Test
  public void testEncryptAndDecryptStreamWithoutAad() throws Exception {
    InputStream plainTextStream = new ByteArrayInputStream(plainText);

    byte[] cipherText = encryptStream(plainTextStream, null, null);

    InputStream inputStream = decryptStream(cipherText, null, null);
    byte[] decryptedPlainText = IOUtils.toByteArray(inputStream);
    assertArrayEquals(plainText, decryptedPlainText);
  }

  @Test
  public void testEncryptAndDecryptStreamWithAad() throws Exception {
    InputStream plainTextStream = new ByteArrayInputStream(plainText);

    byte[] cipherText = encryptStream(plainTextStream, dataAad, keyAad);
    InputStream inputStream = decryptStream(cipherText, dataAad, keyAad);
    byte[] decryptedPlainText = IOUtils.toByteArray(inputStream);
    assertArrayEquals(plainText, decryptedPlainText);
  }

  private byte[] encryptStream(InputStream plainTextStream, byte[] dataAad, byte[] keyAad)
      throws InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException,
          BadPaddingException, NoSuchPaddingException, NoSuchAlgorithmException, IOException {
    InputStream encrypted =
        GcmEncryptionProvider.encrypt(
            meta, plainText.length, plainTextStream, encMat, storageClient, dataAad, keyAad);
    byte[] cipherText = IOUtils.toByteArray(encrypted);
    captureKeysAndIvs();
    return cipherText;
  }

  private InputStream decryptStream(byte[] cipherText, byte[] dataAad, byte[] keyAad)
      throws InvalidKeyException, BadPaddingException, IllegalBlockSizeException,
          InvalidAlgorithmParameterException, NoSuchPaddingException, NoSuchAlgorithmException {
    return GcmEncryptionProvider.decryptStream(
        new ByteArrayInputStream(cipherText),
        Base64.getEncoder().encodeToString(encKekArgumentCaptor.getValue()),
        Base64.getEncoder().encodeToString(cekIvDataArgumentCaptor.getValue()),
        Base64.getEncoder().encodeToString(kekIvDataArgumentCaptor.getValue()),
        encMat,
        dataAad == null ? "" : Base64.getEncoder().encodeToString(dataAad),
        keyAad == null ? "" : Base64.getEncoder().encodeToString(keyAad));
  }

  @Test
  public void testEncryptAndDecryptFileWithoutAad() throws Exception {
    File tempFile = Files.createTempFile("encryption", "").toFile();
    tempFile.deleteOnExit();

    InputStream encrypted =
        new ByteArrayInputStream(encryptStream(new ByteArrayInputStream(plainText), null, null));
    FileUtils.writeByteArrayToFile(tempFile, IOUtils.toByteArray(encrypted));
    captureKeysAndIvs();
    decryptFile(tempFile, null, null);
    byte[] decryptedCipherText = FileUtils.readFileToByteArray(tempFile);
    assertArrayEquals(plainText, decryptedCipherText);
  }

  @Test
  public void testEncryptAndDecryptFileWithAad() throws Exception {
    File tempFile = Files.createTempFile("encryption", "").toFile();
    tempFile.deleteOnExit();

    InputStream encrypted =
        new ByteArrayInputStream(
            encryptStream(new ByteArrayInputStream(plainText), dataAad, keyAad));
    FileUtils.writeByteArrayToFile(tempFile, IOUtils.toByteArray(encrypted));
    captureKeysAndIvs();
    decryptFile(tempFile, dataAad, keyAad);
    byte[] decryptedCipherText = FileUtils.readFileToByteArray(tempFile);
    assertArrayEquals(plainText, decryptedCipherText);
  }

  private void decryptFile(File tempFile, byte[] dataAad, byte[] keyAad)
      throws InvalidKeyException, IllegalBlockSizeException, BadPaddingException,
          InvalidAlgorithmParameterException, IOException, NoSuchPaddingException,
          NoSuchAlgorithmException {
    GcmEncryptionProvider.decryptFile(
        tempFile,
        Base64.getEncoder().encodeToString(encKekArgumentCaptor.getValue()),
        Base64.getEncoder().encodeToString(cekIvDataArgumentCaptor.getValue()),
        Base64.getEncoder().encodeToString(kekIvDataArgumentCaptor.getValue()),
        encMat,
        dataAadArgumentCaptor.getValue() == null
            ? ""
            : Base64.getEncoder().encodeToString(dataAadArgumentCaptor.getValue()),
        keyAadArgumentCaptor.getValue() == null
            ? ""
            : Base64.getEncoder().encodeToString(keyAadArgumentCaptor.getValue()));
  }

  private void captureKeysAndIvs() {
    verify(storageClient)
        .addEncryptionMetadataForGcm(
            storageObjectMetadataArgumentCaptor.capture(),
            matDescArgumentCaptor.capture(),
            encKekArgumentCaptor.capture(),
            cekIvDataArgumentCaptor.capture(),
            kekIvDataArgumentCaptor.capture(),
            keyAadArgumentCaptor.capture(),
            dataAadArgumentCaptor.capture(),
            contentLengthArgumentCaptor.capture());
  }
}
