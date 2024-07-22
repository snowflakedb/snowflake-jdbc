package net.snowflake.client.jdbc.cloud.storage;

import com.google.common.base.Strings;
import java.io.File;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import net.snowflake.common.core.SqlState;

class DecryptionHelper {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeGCSClient.class);

  private final String queryId;
  private final SFBaseSession session;
  private final String key;
  private final String keyIv;
  private final String dataIv;
  private final String keyAad;
  private final String dataAad;
  private final StageInfo.Ciphers ciphers;

  private DecryptionHelper(
      String queryId,
      SFBaseSession session,
      String key,
      String keyIv,
      String dataIv,
      String keyAad,
      String dataAad,
      StageInfo.Ciphers ciphers) {
    this.queryId = queryId;
    this.session = session;
    this.key = key;
    this.keyIv = keyIv;
    this.dataIv = dataIv;
    this.keyAad = keyAad;
    this.dataAad = dataAad;
    this.ciphers = ciphers;
  }

  static DecryptionHelper forCbc(
      String queryId, SFBaseSession session, String key, String contentIv)
      throws SnowflakeSQLLoggedException {
    if (Strings.isNullOrEmpty(key) || Strings.isNullOrEmpty(contentIv)) {
      throw exception(queryId, session);
    }
    return new DecryptionHelper(
        queryId, session, key, null, contentIv, null, null, StageInfo.Ciphers.AESECB_AESCBC);
  }

  static DecryptionHelper forGcm(
      String queryId,
      SFBaseSession session,
      String key,
      String keyIv,
      String dataIv,
      String keyAad,
      String dataAad)
      throws SnowflakeSQLLoggedException {
    if (Strings.isNullOrEmpty(key)
        || Strings.isNullOrEmpty(keyIv)
        || Strings.isNullOrEmpty(dataIv)
        || keyAad == null
        || dataAad == null) {
      throw exception(queryId, session);
    }
    return new DecryptionHelper(
        queryId, session, key, keyIv, dataIv, keyAad, dataAad, StageInfo.Ciphers.AESGCM_AESGCM);
  }

  void validate() throws SnowflakeSQLLoggedException {
    if (key == null
        || dataIv == null
        || (ciphers == StageInfo.Ciphers.AESGCM_AESGCM && keyIv == null)) {
      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "File metadata incomplete");
    }
  }

  void decryptFile(File file, RemoteStoreFileEncryptionMaterial encMat)
      throws SnowflakeSQLLoggedException {
    try {
      switch (ciphers) {
        case AESECB_AESCBC:
          EncryptionProvider.decrypt(file, key, dataIv, encMat);
          break;
        case AESGCM_AESGCM:
          GcmEncryptionProvider.decryptFile(file, key, dataIv, keyIv, encMat, dataAad, keyAad);
          break;
        default:
          throw new IllegalArgumentException("unsupported ciphers: " + ciphers);
      }
    } catch (Exception ex) {
      logger.error("Error decrypting file", ex);
      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "Cannot decrypt file");
    }
  }

  InputStream decryptStream(InputStream inputStream, RemoteStoreFileEncryptionMaterial encMat)
      throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException,
          BadPaddingException, IllegalBlockSizeException, InvalidAlgorithmParameterException {
    switch (ciphers) {
      case AESGCM_AESGCM:
        return GcmEncryptionProvider.decryptStream(
            inputStream, key, dataIv, keyIv, encMat, dataAad, keyAad);
      case AESECB_AESCBC:
        return EncryptionProvider.decryptStream(inputStream, key, dataIv, encMat);
    }
    throw new IllegalArgumentException("unsupported ciphers: " + ciphers);
  }

  private static SnowflakeSQLLoggedException exception(String queryId, SFBaseSession session) {
    return new SnowflakeSQLLoggedException(
        queryId,
        session,
        ErrorCode.INTERNAL_ERROR.getMessageCode(),
        SqlState.INTERNAL_ERROR,
        "File metadata incomplete");
  }
}
