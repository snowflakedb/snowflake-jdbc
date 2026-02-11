package net.snowflake.client.internal.jdbc.cloud.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.InputStream;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import net.snowflake.client.internal.core.ObjectMapperFactory;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.exception.SnowflakeSQLLoggedException;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import net.snowflake.common.core.SqlState;
import net.snowflake.floe.FloeParameterSpec;

interface DecryptionHelper {
  InputStream decryptStream(InputStream inputStream) throws Exception;

  void decryptFile(File file) throws Exception;
}

// This interface's purpose is to provide metadata for CBC, which are CSP dependant.
interface CbcMetadataProvider {
  String getKey();

  String getIv();
}

class EncryptionHelperFactory {
  public static final String formatVersionKey = "sf-client-side-encryption-format-version";
  public static final String formatVersion10 = "1.0";

  static DecryptionHelper from(
      String queryId,
      SFBaseSession session,
      Map<String, String> metaMap,
      CbcMetadataProvider cbcMetadataProvider,
      RemoteStoreFileEncryptionMaterial encMat)
      throws Exception {
    if (metaMap.containsKey(formatVersionKey)) {
      if (formatVersion10.equals(metaMap.get(formatVersionKey))) {
        return new GcmDecryptionHelper(queryId, session, metaMap, encMat);
      } else {
        throw new SnowflakeSQLLoggedException(
            queryId,
            session,
            StorageHelper.getOperationException(StorageHelper.DOWNLOAD).getMessageCode(),
            SqlState.INTERNAL_ERROR,
            "unsupported " + formatVersionKey + ": " + metaMap.get(formatVersionKey));
      }
    } else {
      return new CbcDecryptionHandler(queryId, session, metaMap, cbcMetadataProvider, encMat);
    }
  }
}

class CbcDecryptionHandler implements DecryptionHelper {
  private final String key;
  private final String iv;
  private final RemoteStoreFileEncryptionMaterial encMat;

  CbcDecryptionHandler(
      String queryId,
      SFBaseSession session,
      Map<String, String> metaMap,
      CbcMetadataProvider cbcMetadataProvider,
      RemoteStoreFileEncryptionMaterial encMat)
      throws SnowflakeSQLLoggedException {
    this.key = cbcMetadataProvider.getKey();
    this.iv = cbcMetadataProvider.getIv();
    this.encMat = encMat;
    if (key == null || iv == null) {
      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          StorageHelper.getOperationException(StorageHelper.DOWNLOAD).getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "File metadata incomplete");
    }
  }

  @Override
  public InputStream decryptStream(InputStream inputStream) throws Exception {
    return CbcEncryptionProvider.decryptStream(inputStream, key, iv, encMat);
  }

  @Override
  public void decryptFile(File file) throws Exception {
    CbcEncryptionProvider.decryptFile(file, key, iv, encMat);
  }
}

class GcmDecryptionHelper implements DecryptionHelper {
  static final String keyCipherKey = "sf-client-side-encryption-key-cipher";
  static final String keyEncryptionParametersKey =
      "sf-client-side-encryption-key-encryption-parameters";
  static final String keyKey = "sf-client-side-encryption-key";
  static final String contentCipherKey = "sf-client-side-encryption-content-cipher";
  static final String contentEncryptionParametersKey =
      "sf-client-side-encryption-content-encryption-parameters";
  static final ObjectMapper objectMapper = ObjectMapperFactory.getObjectMapper();

  private final RemoteStoreFileEncryptionMaterial encMat;
  private final byte[] keyIv;
  private final byte[] keyAad;
  private final byte[] encryptedKey;
  private final byte[] contentAad;
  private final byte[] contentHeader;

  public GcmDecryptionHelper(
      String queryId,
      SFBaseSession session,
      Map<String, String> metaMap,
      RemoteStoreFileEncryptionMaterial encMat)
      throws Exception {
    this.encMat = encMat;
    if (!"AES_GCM".equals(metaMap.get(keyCipherKey))) {
      throw incompleteMetadataException(queryId, session);
    }
    if (!"FLOE".equals(metaMap.get(contentCipherKey))) {
      throw incompleteMetadataException(queryId, session);
    }
    if (Stream.of(
            metaMap.get(keyEncryptionParametersKey), metaMap.get(contentEncryptionParametersKey))
        .anyMatch(Objects::isNull)) {
      throw incompleteMetadataException(queryId, session);
    }
    this.encryptedKey =
        Optional.ofNullable(metaMap.get(keyKey))
            .map(v -> Base64.getDecoder().decode(v))
            .orElseThrow(() -> incompleteMetadataException(queryId, session));
    Map<String, String> keyParameters =
        objectMapper.readerFor(Map.class).readValue(metaMap.get(keyEncryptionParametersKey));
    this.keyIv =
        Optional.ofNullable(keyParameters.get("iv"))
            .map(v -> Base64.getDecoder().decode(v))
            .orElseThrow(() -> incompleteMetadataException(queryId, session));
    this.keyAad =
        Optional.ofNullable(keyParameters.get("aad"))
            .map(v -> Base64.getDecoder().decode(v))
            .orElseThrow(() -> incompleteMetadataException(queryId, session));
    Map<String, String> contentParameters =
        objectMapper.readerFor(Map.class).readValue(metaMap.get(keyEncryptionParametersKey));
    this.contentHeader =
        Optional.ofNullable(contentParameters.get("header"))
            .map(v -> Base64.getDecoder().decode(v))
            .orElseThrow(() -> incompleteMetadataException(queryId, session));
    this.contentAad =
        Optional.ofNullable(contentParameters.get("aad"))
            .map(v -> Base64.getDecoder().decode(v))
            .orElseThrow(() -> incompleteMetadataException(queryId, session));
  }

  private static SnowflakeSQLLoggedException incompleteMetadataException(
      String queryId, SFBaseSession session) {
    return new SnowflakeSQLLoggedException(
        queryId,
        session,
        StorageHelper.getOperationException(StorageHelper.DOWNLOAD).getMessageCode(),
        SqlState.INTERNAL_ERROR,
        "File metadata incomplete");
  }

  @Override
  public InputStream decryptStream(InputStream inputStream) throws Exception {
    FloeParameterSpec floeParameterSpec = FloeParameterSpec.fromHeader(contentHeader);
    byte[] contentKey =
        GcmEncryptionProvider.decrypt(
            Base64.getDecoder().decode(encMat.getQueryStageMasterKey()),
            encryptedKey,
            keyIv,
            keyAad);
    return FloeEncryptionProvider.decryptStream(
        inputStream, contentKey, floeParameterSpec, contentAad, contentHeader);
  }

  @Override
  public void decryptFile(File file) throws Exception {
    FloeParameterSpec floeParameterSpec = FloeParameterSpec.fromHeader(contentHeader);
    byte[] contentKey =
        GcmEncryptionProvider.decrypt(
            Base64.getDecoder().decode(encMat.getQueryStageMasterKey()),
            encryptedKey,
            keyIv,
            keyAad);
    FloeEncryptionProvider.decryptFile(
        file, contentKey, floeParameterSpec, contentAad, contentHeader);
  }
}
