package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.CipherInputStream;
import net.snowflake.client.jdbc.MatDesc;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class EncryptionProviderTest {
  private final SecureRandom random = new SecureRandom();

  private final ArgumentCaptor<StorageObjectMetadata> storageObjectMetadataArgumentCaptor =
      ArgumentCaptor.forClass(StorageObjectMetadata.class);
  private final ArgumentCaptor<MatDesc> matDescArgumentCaptor =
      ArgumentCaptor.forClass(MatDesc.class);
  private final ArgumentCaptor<byte[]> ivDataArgumentCaptor = ArgumentCaptor.forClass(byte[].class);
  private final ArgumentCaptor<byte[]> encKekArgumentCaptor = ArgumentCaptor.forClass(byte[].class);
  private final ArgumentCaptor<Long> contentLengthArgumentCaptor =
      ArgumentCaptor.forClass(Long.class);

  private final StorageObjectMetadata meta = mock(StorageObjectMetadata.class);
  private final SnowflakeStorageClient storageClient = mock(SnowflakeStorageClient.class);

  private final String queryStageMasterKey =
      Base64.getEncoder().encodeToString(random.generateSeed(16));
  private final RemoteStoreFileEncryptionMaterial encMat = new RemoteStoreFileEncryptionMaterial();

  byte[] plainText = "the quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8);

  @BeforeEach
  public void setUp() {
    encMat.setQueryStageMasterKey(queryStageMasterKey);
    encMat.setSmkId(123);
    encMat.setQueryId("query-id");
  }

  @Test
  public void testEncryptAndDecryptStream() throws Exception {
    InputStream plainTextStream = new ByteArrayInputStream(plainText);

    CipherInputStream encrypted =
        EncryptionProvider.encrypt(meta, plainText.length, plainTextStream, encMat, storageClient);
    byte[] cipherText = IOUtils.toByteArray(encrypted);
    verify(storageClient)
        .addEncryptionMetadata(
            storageObjectMetadataArgumentCaptor.capture(),
            matDescArgumentCaptor.capture(),
            ivDataArgumentCaptor.capture(),
            encKekArgumentCaptor.capture(),
            contentLengthArgumentCaptor.capture());

    InputStream inputStream =
        EncryptionProvider.decryptStream(
            new ByteArrayInputStream(cipherText),
            Base64.getEncoder().encodeToString(encKekArgumentCaptor.getValue()),
            Base64.getEncoder().encodeToString(ivDataArgumentCaptor.getValue()),
            encMat);
    byte[] decryptedPlainText = IOUtils.toByteArray(inputStream);
    assertArrayEquals(plainText, decryptedPlainText);
  }

  @Test
  public void testEncryptAndDecryptFile() throws Exception {
    File tempFile = Files.createTempFile("encryption", "").toFile();
    tempFile.deleteOnExit();

    CipherInputStream encrypted =
        EncryptionProvider.encrypt(
            meta, plainText.length, new ByteArrayInputStream(plainText), encMat, storageClient);
    FileUtils.writeByteArrayToFile(tempFile, IOUtils.toByteArray(encrypted));
    verify(storageClient)
        .addEncryptionMetadata(
            storageObjectMetadataArgumentCaptor.capture(),
            matDescArgumentCaptor.capture(),
            ivDataArgumentCaptor.capture(),
            encKekArgumentCaptor.capture(),
            contentLengthArgumentCaptor.capture());

    EncryptionProvider.decrypt(
        tempFile,
        Base64.getEncoder().encodeToString(encKekArgumentCaptor.getValue()),
        Base64.getEncoder().encodeToString(ivDataArgumentCaptor.getValue()),
        encMat);
    byte[] decryptedCipherText = FileUtils.readFileToByteArray(tempFile);
    assertArrayEquals(plainText, decryptedCipherText);
  }
}
