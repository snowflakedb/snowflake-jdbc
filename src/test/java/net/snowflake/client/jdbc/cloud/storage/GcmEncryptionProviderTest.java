package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import javax.crypto.AEADBadTagException;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import net.snowflake.client.jdbc.MatDesc;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class GcmEncryptionProviderTest {
  private final SecureRandom random = new SecureRandom();

  private final ArgumentCaptor<StorageObjectMetadata> storageObjectMetadataArgumentCaptor =
      ArgumentCaptor.forClass(StorageObjectMetadata.class);
  private final ArgumentCaptor<MatDesc> matDescArgumentCaptor =
      ArgumentCaptor.forClass(MatDesc.class);
  private final ArgumentCaptor<byte[]> dataIvDataArgumentCaptor =
      ArgumentCaptor.forClass(byte[].class);
  private final ArgumentCaptor<byte[]> keyIvDataArgumentCaptor =
      ArgumentCaptor.forClass(byte[].class);
  private final ArgumentCaptor<byte[]> encKeyArgumentCaptor = ArgumentCaptor.forClass(byte[].class);
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

  @BeforeEach
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

  @Test
  public void testDecryptStreamWithInvalidKeyAad() throws Exception {
    InputStream plainTextStream = new ByteArrayInputStream(plainText);

    byte[] cipherText = encryptStream(plainTextStream, dataAad, keyAad);
    assertThrows(
        AEADBadTagException.class, () -> decryptStream(cipherText, dataAad, new byte[] {'a'}));
  }

  @Test
  public void testDecryptStreamWithInvalidDataAad() throws Exception {
    InputStream plainTextStream = new ByteArrayInputStream(plainText);

    byte[] cipherText = encryptStream(plainTextStream, dataAad, keyAad);
    IOException ioException =
        assertThrows(
            IOException.class,
            () -> IOUtils.toByteArray(decryptStream(cipherText, new byte[] {'a'}, keyAad)));
    assertEquals(ioException.getCause().getClass(), AEADBadTagException.class);
  }

  @Test
  public void testDecryptStreamWithInvalidCipherText() throws Exception {
    InputStream plainTextStream = new ByteArrayInputStream(plainText);

    byte[] cipherText = encryptStream(plainTextStream, dataAad, keyAad);
    cipherText[0] = (byte) ((cipherText[0] + 1) % 255);
    IOException ioException =
        assertThrows(
            IOException.class,
            () -> IOUtils.toByteArray(decryptStream(cipherText, dataAad, keyAad)));
    assertEquals(ioException.getCause().getClass(), AEADBadTagException.class);
  }

  @Test
  public void testDecryptStreamWithInvalidTag() throws Exception {
    InputStream plainTextStream = new ByteArrayInputStream(plainText);

    byte[] cipherText = encryptStream(plainTextStream, dataAad, keyAad);
    cipherText[cipherText.length - 1] = (byte) ((cipherText[cipherText.length - 1] + 1) % 255);
    IOException ioException =
        assertThrows(
            IOException.class,
            () -> IOUtils.toByteArray(decryptStream(cipherText, dataAad, keyAad)));
    assertEquals(ioException.getCause().getClass(), AEADBadTagException.class);
  }

  @Test
  public void testDecryptStreamWithInvalidKey() throws Exception {
    InputStream plainTextStream = new ByteArrayInputStream(plainText);

    byte[] cipherText = encryptStream(plainTextStream, dataAad, keyAad);

    byte[] encryptedKey = encKeyArgumentCaptor.getValue();
    encryptedKey[0] = (byte) ((encryptedKey[0] + 1) % 255);
    assertThrows(
        AEADBadTagException.class,
        () ->
            IOUtils.toByteArray(
                GcmEncryptionProvider.decryptStream(
                    new ByteArrayInputStream(cipherText),
                    Base64.getEncoder().encodeToString(encryptedKey),
                    Base64.getEncoder().encodeToString(dataIvDataArgumentCaptor.getValue()),
                    Base64.getEncoder().encodeToString(keyIvDataArgumentCaptor.getValue()),
                    encMat,
                    dataAad == null ? "" : Base64.getEncoder().encodeToString(dataAad),
                    keyAad == null ? "" : Base64.getEncoder().encodeToString(keyAad))));
  }

  @Test
  public void testDecryptStreamWithInvalidDataIV() throws Exception {
    InputStream plainTextStream = new ByteArrayInputStream(plainText);

    byte[] cipherText = encryptStream(plainTextStream, dataAad, keyAad);
    byte[] dataIvBase64 = dataIvDataArgumentCaptor.getValue();
    dataIvBase64[0] = (byte) ((dataIvBase64[0] + 1) % 255);
    IOException ioException =
        assertThrows(
            IOException.class,
            () -> {
              IOUtils.toByteArray(
                  GcmEncryptionProvider.decryptStream(
                      new ByteArrayInputStream(cipherText),
                      Base64.getEncoder().encodeToString(encKeyArgumentCaptor.getValue()),
                      Base64.getEncoder().encodeToString(dataIvBase64),
                      Base64.getEncoder().encodeToString(keyIvDataArgumentCaptor.getValue()),
                      encMat,
                      dataAad == null ? "" : Base64.getEncoder().encodeToString(dataAad),
                      keyAad == null ? "" : Base64.getEncoder().encodeToString(keyAad)));
            });
    assertEquals(ioException.getCause().getClass(), AEADBadTagException.class);
  }

  @Test
  public void testDecryptStreamWithInvalidKeyIV() throws Exception {
    InputStream plainTextStream = new ByteArrayInputStream(plainText);

    byte[] cipherText = encryptStream(plainTextStream, dataAad, keyAad);
    byte[] keyIvBase64 = keyIvDataArgumentCaptor.getValue();
    keyIvBase64[0] = (byte) ((keyIvBase64[0] + 1) % 255);
    assertThrows(
        AEADBadTagException.class,
        () -> {
          IOUtils.toByteArray(
              GcmEncryptionProvider.decryptStream(
                  new ByteArrayInputStream(cipherText),
                  Base64.getEncoder().encodeToString(encKeyArgumentCaptor.getValue()),
                  Base64.getEncoder().encodeToString(dataIvDataArgumentCaptor.getValue()),
                  Base64.getEncoder().encodeToString(keyIvBase64),
                  encMat,
                  dataAad == null ? "" : Base64.getEncoder().encodeToString(dataAad),
                  keyAad == null ? "" : Base64.getEncoder().encodeToString(keyAad)));
        });
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
        Base64.getEncoder().encodeToString(encKeyArgumentCaptor.getValue()),
        Base64.getEncoder().encodeToString(dataIvDataArgumentCaptor.getValue()),
        Base64.getEncoder().encodeToString(keyIvDataArgumentCaptor.getValue()),
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
        Base64.getEncoder().encodeToString(encKeyArgumentCaptor.getValue()),
        Base64.getEncoder().encodeToString(dataIvDataArgumentCaptor.getValue()),
        Base64.getEncoder().encodeToString(keyIvDataArgumentCaptor.getValue()),
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
            encKeyArgumentCaptor.capture(),
            dataIvDataArgumentCaptor.capture(),
            keyIvDataArgumentCaptor.capture(),
            keyAadArgumentCaptor.capture(),
            dataAadArgumentCaptor.capture(),
            contentLengthArgumentCaptor.capture());
  }
}
