package net.snowflake.client.core;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Interface for accessing Platform specific Local Secure Storage E.g. keychain on Mac credential
 * manager on Windows
 */
interface SecureStorageManager {
  int COLON_CHAR_LENGTH = 1;

  SecureStorageStatus setCredential(String host, String user, String type, String token);

  String getCredential(String host, String user, String type);

  SecureStorageStatus deleteCredential(String host, String user, String type);

  static String buildCredentialsKey(String host, String user, String type) {
    StringBuilder target =
        new StringBuilder(host.length() + user.length() + type.length() + 3 * COLON_CHAR_LENGTH);

    target.append(host.toUpperCase());
    target.append(":");
    target.append(user.toUpperCase());
    target.append(":");
    target.append(type.toUpperCase());

    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] hash = md.digest(target.toString().getBytes());
      return HexUtil.byteToHexString(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  enum SecureStorageStatus {
    NOT_FOUND,
    FAILURE,
    SUCCESS,
    UNSUPPORTED
  }
}
