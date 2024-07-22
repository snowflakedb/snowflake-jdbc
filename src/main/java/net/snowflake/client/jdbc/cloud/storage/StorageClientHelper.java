package net.snowflake.client.jdbc.cloud.storage;

import static net.snowflake.client.jdbc.cloud.storage.SnowflakeS3Client.AMZ_CIPHER;
import static net.snowflake.client.jdbc.cloud.storage.SnowflakeS3Client.AMZ_DATA_AAD;
import static net.snowflake.client.jdbc.cloud.storage.SnowflakeS3Client.AMZ_DATA_IV;
import static net.snowflake.client.jdbc.cloud.storage.SnowflakeS3Client.AMZ_KEY;
import static net.snowflake.client.jdbc.cloud.storage.SnowflakeS3Client.AMZ_KEY_AAD;
import static net.snowflake.client.jdbc.cloud.storage.SnowflakeS3Client.AMZ_KEY_IV;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Map;
import java.util.Optional;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import net.snowflake.common.core.SqlState;

class StorageClientHelper {
  private static final byte[] keyAad = "".getBytes(StandardCharsets.UTF_8);
  private static final byte[] dataAad = "".getBytes(StandardCharsets.UTF_8);

  private final SnowflakeStorageClient client;
  private final RemoteStoreFileEncryptionMaterial encMat;
  private final SFBaseSession session;
  private final StageInfo.Ciphers ciphers;

  StorageClientHelper(
      SnowflakeStorageClient client,
      RemoteStoreFileEncryptionMaterial encMat,
      SFBaseSession session,
      StageInfo.Ciphers ciphers) {
    this.client = client;
    this.encMat = encMat;
    this.session = session;
    this.ciphers = ciphers;
  }

  String buildEncryptionMetadataJSONForEcbCbc(String iv64, String key64) {
    return String.format(
        "{\"EncryptionMode\":\"FullBlob\",\"WrappedContentKey\""
            + ":{\"KeyId\":\"symmKey1\",\"EncryptedKey\":\"%s\""
            + ",\"Algorithm\":\"AES_CBC_256\"},\"EncryptionAgent\":"
            + "{\"Protocol\":\"1.0\",\"EncryptionAlgorithm\":"
            + "\"AES_CBC_256\"},\"ContentEncryptionIV\":\"%s\""
            + ",\"KeyWrappingMetadata\":{\"EncryptionLibrary\":"
            + "\"Java 5.3.0\"}}",
        key64, iv64);
  }

  String buildEncryptionMetadataJSONForGcm(
      String keyIv, String key, String contentIv, String fileKeyAad, String contentAad) {
    return String.format(
        "{"
            + "\"EncryptionMode\":\"FullBlob\","
            + "\"WrappedContentKey\":{"
            + "\"KeyId\":\"symmKey1\","
            + "\"EncryptedKey\":\"%s\","
            + "\"Algorithm\":\"AES_GCM\","
            + "\"KeyEncryptionIV\":\"%s\","
            + "\"FileKeyAad\":\"%s\""
            + "},"
            + "\"EncryptionAgent\":{"
            + "\"Protocol\":\"1.0\","
            + "\"EncryptionAlgorithm\":\"AES_GCM\""
            + "},"
            + "\"ContentEncryptionIV\":\"%s\","
            + "\"ContentAad\":\"%s\","
            + "\"KeyWrappingMetadata\":{"
            + "\"EncryptionLibrary\":\"Java 5.3.0\""
            + "}"
            + "}",
        key, keyIv, fileKeyAad, contentIv, contentAad);
  }

  DecryptionHelper buildEncryptionMetadataFromAwsMetadata(
      String queryId, Map<String, String> metaMap) throws SnowflakeSQLLoggedException {
    StageInfo.Ciphers ciphers =
        Optional.ofNullable(metaMap.get(AMZ_CIPHER))
            .map(this::toCiphers)
            .orElse(StageInfo.Ciphers.AESECB_AESCBC);
    String key = metaMap.get(AMZ_KEY);
    String keyIv = metaMap.get(AMZ_KEY_IV);
    String dataIv = metaMap.get(AMZ_DATA_IV);
    String keyAad = metaMap.get(AMZ_KEY_AAD);
    String dataAad = metaMap.get(AMZ_DATA_AAD);
    switch (ciphers) {
      case AESECB_AESCBC:
        return DecryptionHelper.forCbc(queryId, session, key, dataIv);
      case AESGCM_AESGCM:
        return DecryptionHelper.forGcm(queryId, session, key, keyIv, dataIv, keyAad, dataAad);
    }
    throw new IllegalArgumentException("unknown cipher " + ciphers);
  }

  private StageInfo.Ciphers toCiphers(String s) {
    switch (s) {
      case "AES_CBC":
      case "":
        return StageInfo.Ciphers.AESECB_AESCBC;
      case "AES_GCM":
        return StageInfo.Ciphers.AESGCM_AESGCM;
    }
    throw new IllegalArgumentException("Unknown cipher " + s);
  }

  DecryptionHelper parseEncryptionDataFromJson(String jsonEncryptionData, String queryId)
      throws SnowflakeSQLException {
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    JsonFactory factory = mapper.getFactory();
    try {
      JsonParser parser = factory.createParser(jsonEncryptionData);
      JsonNode encryptionDataNode = mapper.readTree(parser);

      String contentIv = encryptionDataNode.get("ContentEncryptionIV").asText();
      String contentAad =
          Optional.ofNullable(encryptionDataNode.get("ContentAad"))
              .map(JsonNode::asText)
              .orElse(null);
      String key = encryptionDataNode.get("WrappedContentKey").get("EncryptedKey").asText();
      String keyIv =
          Optional.ofNullable(encryptionDataNode.get("WrappedContentKey"))
              .map(v -> v.get("KeyEncryptionIV"))
              .map(JsonNode::asText)
              .orElse(null);
      String fileKeyAad =
          Optional.ofNullable(encryptionDataNode.get("WrappedContentKey"))
              .map(v -> v.get("FileKeyAad"))
              .map(JsonNode::asText)
              .orElse(null);

      String algorithm = encryptionDataNode.get("WrappedContentKey").get("Algorithm").asText();
      if (algorithm.contains("AES_GCM")) {
        return DecryptionHelper.forGcm(
            queryId, session, key, keyIv, contentIv, fileKeyAad, contentAad);
      } else {
        return DecryptionHelper.forCbc(queryId, session, key, contentIv);
      }

    } catch (Exception ex) {
      throw new SnowflakeSQLException(
          queryId,
          ex,
          SqlState.SYSTEM_ERROR,
          ErrorCode.IO_ERROR.getMessageCode(),
          "Error parsing encryption data as json" + ": " + ex.getMessage());
    }
  }

  InputStream encrypt(
      StorageObjectMetadata meta, long originalContentLength, InputStream uploadStream)
      throws InvalidKeyException, InvalidAlgorithmParameterException, NoSuchAlgorithmException,
          NoSuchProviderException, NoSuchPaddingException, FileNotFoundException,
          IllegalBlockSizeException, BadPaddingException {
    if (ciphers == null || ciphers == StageInfo.Ciphers.AESECB_AESCBC) {
      return EncryptionProvider.encrypt(meta, originalContentLength, uploadStream, encMat, client);
    } else {
      return GcmEncryptionProvider.encrypt(
          meta, originalContentLength, uploadStream, encMat, client, dataAad, keyAad);
    }
  }
}
