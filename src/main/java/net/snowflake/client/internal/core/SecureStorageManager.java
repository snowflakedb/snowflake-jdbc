package net.snowflake.client.internal.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.TreeMap;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;

/**
 * Interface for accessing Platform specific Local Secure Storage E.g. keychain on Mac credential
 * manager on Windows
 */
interface SecureStorageManager {

  SecureStorageStatus setCredential(String cacheKey, String token);

  String getCredential(String cacheKey);

  SecureStorageStatus deleteCredential(String cacheKey);

  /**
   * Builds a versioned, SHA-256-hashed canonical-JSON cache key from {@code input}.
   *
   * <p>The returned string has the form {@code SnowflakeTokenCache.v2.<lowercase-hex-sha256>} and
   * is identical across all Snowflake drivers that implement the same spec.
   *
   * @throws IllegalArgumentException if {@code input.snowflake} or {@code input.username} is empty
   */
  static String buildCacheKey(CacheKeyInput input) {
    if (input.snowflake == null || input.snowflake.isEmpty()) {
      throw new IllegalArgumentException("snowflake URL must not be empty");
    }
    if (input.username == null || input.username.isEmpty()) {
      throw new IllegalArgumentException("username must not be empty");
    }

    TreeMap<String, String> keyData = new TreeMap<>();
    keyData.put("idp", normalizeUrl(input.idp));
    keyData.put("role", normalizeIdentifier(input.role));
    keyData.put("snowflake", normalizeUrl(input.snowflake));
    keyData.put("token_type", input.tokenType);
    keyData.put("username", normalizeIdentifier(input.username));

    String json;
    try {
      json = new ObjectMapper().writeValueAsString(keyData);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize cache key to JSON", e);
    }

    try {
      byte[] hash =
          MessageDigest.getInstance("SHA-256").digest(json.getBytes(StandardCharsets.UTF_8));
      return "SnowflakeTokenCache.v2." + SnowflakeUtil.byteToHexStringLower(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Normalizes a URL for inclusion in a cache key.
   *
   * <ol>
   *   <li>Strips {@code https://} or {@code http://} scheme if present.
   *   <li>Strips optional userinfo ({@code user:pass@} prefix after scheme removal).
   *   <li>Drops query string and fragment.
   *   <li>Trims a root-only trailing slash (bare host or host:port only).
   *   <li>Uppercases the remainder.
   * </ol>
   */
  static String normalizeUrl(String url) {
    if (url == null) {
      return "";
    }
    String s = url.replaceFirst("(?i)^https?://", "");
    int at = s.indexOf('@');
    if (at >= 0) {
      s = s.substring(at + 1);
    }
    int q = s.indexOf('?');
    if (q >= 0) {
      s = s.substring(0, q);
    }
    int hash = s.indexOf('#');
    if (hash >= 0) {
      s = s.substring(0, hash);
    }
    // Trim trailing slash(es) — bare host has no slash suffix.
    while (s.endsWith("/")) {
      s = s.substring(0, s.length() - 1);
    }
    return s.toUpperCase(Locale.ROOT);
  }

  /**
   * Normalizes a Snowflake identifier (username or role) for inclusion in a cache key.
   *
   * <p>Characters outside double-quoted segments are uppercased; characters inside {@code "…"}
   * segments are preserved verbatim (including the surrounding quotes).
   */
  static String normalizeIdentifier(String id) {
    if (id == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder(id.length());
    boolean inQuotes = false;
    for (char c : id.toCharArray()) {
      if (c == '"') {
        inQuotes = !inQuotes;
        sb.append(c);
      } else if (inQuotes) {
        sb.append(c);
      } else {
        sb.append(Character.toUpperCase(c));
      }
    }
    return sb.toString();
  }

  enum SecureStorageStatus {
    NOT_FOUND,
    FAILURE,
    SUCCESS,
    UNSUPPORTED
  }
}
