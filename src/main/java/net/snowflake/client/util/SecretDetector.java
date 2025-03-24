package net.snowflake.client.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONStyle;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/** Search for credentials in sql and/or other text */
public class SecretDetector {
  // "\\s*" refers to >= 0 spaces, "[^']" refers to chars other than `'`
  private static final Pattern AWS_KEY_PATTERN =
      Pattern.compile(
          "(aws_key_id|aws_secret_key|access_key_id|secret_access_key)(\\s*=\\s*)'([^']+)'",
          Pattern.CASE_INSENSITIVE);

  // Used for detecting tokens in serialized JSON
  private static final Pattern AWS_TOKEN_PATTERN =
      Pattern.compile(
          "(accessToken|tempToken|keySecret)\"\\s*:\\s*\"([a-z0-9/+]{32,}={0,2})\"",
          Pattern.CASE_INSENSITIVE);

  // Used for detecting OAuth tokens in serialized JSON
  private static final Pattern OAUTH_JSON_PATTERN =
      Pattern.compile(
          "(access_token|refresh_token)\""
              + "\\s*:\\s*"
              + "\"([a-zA-Z0-9!\"#$%&'\\()*+,-./:;<=>?@\\[\\]^_`\\{|\\}~]{3,})\"",
          Pattern.CASE_INSENSITIVE);

  // Signature added in the query string of a URL in SAS based authentication
  // for S3 or Azure Storage requests
  private static final Pattern SAS_TOKEN_PATTERN =
      Pattern.compile(
          "(sig|signature|AWSAccessKeyId|password|passcode)=([a-z0-9%/+]{16,})",
          Pattern.CASE_INSENSITIVE);

  // Search for password pattern
  private static final Pattern PASSWORD_PATTERN =
      Pattern.compile(
          "(password|passcode|pwd)"
              + "([\'\"\\s:=]+)"
              + "([a-z0-9!\"#$%&'\\()*+,-./:;<=>?@\\[\\]^_`\\{|\\}~]{6,})",
          Pattern.CASE_INSENSITIVE);

  // "-----BEGIN PRIVATE KEY-----  // #pragma: allowlist secret
  // [PRIVATE-KEY]
  // -----END PRIVATE KEY-----"
  private static final Pattern PRIVATE_KEY_PATTERN =
      Pattern.compile(
          "-----BEGIN PRIVATE KEY-----" // #pragma: allowlist secret
              + "\\\\n([a-z0-9/+=\\\\n]{32,})\\\\n"
              + "-----END PRIVATE KEY-----",
          Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

  // "privateKeyData": "[PRIVATE-KEY]"
  private static final Pattern PRIVATE_KEY_DATA_PATTERN =
      Pattern.compile(
          "\"privateKeyData\": \"([a-z0-9/+=\\\\n]{10,})\"",
          Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

  private static final Pattern CONNECTION_TOKEN_PATTERN =
      Pattern.compile(
          "(token|assertion content)" + "(['\"\\s:=]+)" + "([a-z0-9=/_\\-+]{8,})",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern ENCRYPTION_MATERIAL_PATTERN =
      Pattern.compile("\"encryptionMaterial\"\\s*:\\s*\\{.*?\\}", Pattern.CASE_INSENSITIVE);

  // only attempt to find secrets in its leading 100Kb SNOW-30961
  private static final int MAX_LENGTH = 100 * 1000;

  private static String[] SENSITIVE_NAMES = {
    "access_key_id",
    "accesstoken",
    "aws_key_id",
    "aws_secret_key",
    "awsaccesskeyid",
    "keysecret",
    "passcode",
    "password",
    "privatekey",
    "privatekeydata",
    "secret_access_key",
    "sig",
    "signature",
    "temptoken",
    "oauthClientId",
    "oauthClientSecret",
    "accessToken",
    "refreshToken"
  };

  private static Set<String> SENSITIVE_NAME_SET = new HashSet<>(Arrays.asList(SENSITIVE_NAMES));

  /**
   * Check whether the name is sensitive
   *
   * @param name the name
   * @return true if the name is sensitive.
   */
  public static boolean isSensitive(String name) {
    return SENSITIVE_NAME_SET.contains(name.toLowerCase());
  }

  /**
   * Determine whether a connection parameter should be masked if parameter values are being
   * printed. Sensitive parameters include passwords and token values. Helper function for
   * maskParameterValue().
   *
   * @param name of the parameter
   * @return true if the parameter should be masked
   */
  private static boolean isSensitiveParameter(String name) {
    Pattern PASSWORD_IN_NAME =
        Pattern.compile(
            ".*?(password|pwd|token|proxyuser|privatekey|passcode|proxypassword|private_key_base|oauthClientSecret|oauthClientId).*?",
            Pattern.CASE_INSENSITIVE);
    Matcher matcher = PASSWORD_IN_NAME.matcher(name);
    return isSensitive(name) || matcher.matches();
  }

  /**
   * Mask sensitive parameter values. Used currently for connection parameters whose values are to
   * be recorded for each session.
   *
   * @param key parameter key
   * @param value parameter value, which is sometimes masked
   * @return the original value if the parameter key does not mark it as sensitive, or return a
   *     masked text if the key is determined to be sensitive.
   */
  public static String maskParameterValue(String key, String value) {
    if (isSensitiveParameter(key)) {
      return "****";
    }
    return value;
  }

  private static String filterAWSKeys(String text) {
    Matcher matcher =
        AWS_KEY_PATTERN.matcher(text.length() <= MAX_LENGTH ? text : text.substring(0, MAX_LENGTH));

    if (matcher.find()) {
      return matcher.replaceAll("$1$2'****'");
    }
    return text;
  }

  private static String filterSASTokens(String text) {
    Matcher matcher =
        SAS_TOKEN_PATTERN.matcher(
            text.length() <= MAX_LENGTH ? text : text.substring(0, MAX_LENGTH));

    if (matcher.find()) {
      return matcher.replaceAll("$1=****");
    }
    return text;
  }

  private static String filterPassword(String text) {
    Matcher matcher =
        PASSWORD_PATTERN.matcher(
            text.length() <= MAX_LENGTH ? text : text.substring(0, MAX_LENGTH));

    if (matcher.find()) {
      return matcher.replaceAll("$1$2**** ");
    }
    return text;
  }

  private static String filterConnectionTokens(String text) {
    Matcher matcher =
        CONNECTION_TOKEN_PATTERN.matcher(
            text.length() <= MAX_LENGTH ? text : text.substring(0, MAX_LENGTH));

    if (matcher.find()) {
      return matcher.replaceAll("$1$2****");
    }
    return text;
  }

  /**
   * mask AWS secret in the input string
   *
   * @param sql The sql text to mask
   * @return masked string
   */
  public static String maskAWSSecret(String sql) {
    return filterAWSKeys(sql);
  }

  /**
   * Masks SAS token(s) in the input string
   *
   * @param text Text which may contain SAS token(s)
   * @return Masked string
   */
  public static String maskSASToken(String text) {
    return filterSASTokens(text);
  }

  /**
   * Masks any secrets present in the input string. This currently checks for SAS tokens ({@link
   * SecretDetector#maskSASToken(String)}) and AWS keys ({@link
   * SecretDetector#maskAWSSecret(String)}.
   *
   * @param text Text which may contain secrets
   * @return Masked string
   */
  public static String maskSecrets(String text) {
    return filterAccessTokens(
        filterConnectionTokens(
            filterPassword(
                filterSASTokens(
                    filterAWSKeys(filterOAuthTokens(filterEncryptionMaterial(text)))))));
  }

  /**
   * Masks any secrets present in the OAuth token request JSON response.
   *
   * @param text Text which may contain secrets
   * @return Masked string
   */
  @SnowflakeJdbcInternalApi
  public static String filterOAuthTokens(String text) {
    Matcher matcher =
        OAUTH_JSON_PATTERN.matcher(
            text.length() <= MAX_LENGTH ? text : text.substring(0, MAX_LENGTH));

    if (matcher.find()) {
      return matcher.replaceAll("$1\":\"****\"");
    }
    return text;
  }

  /**
   * Filter access tokens that might be buried in JSON. Currently only used to filter the
   * scopedCreds passed for XP binary downloads
   *
   * @param message the message text which may contain secrets
   * @return Return filtered message
   */
  public static String filterAccessTokens(String message) {
    Matcher awsMatcher = AWS_TOKEN_PATTERN.matcher(message);

    // aws
    if (awsMatcher.find()) {
      message = awsMatcher.replaceAll("$1\":\"XXXX\"");
    }

    // azure
    Matcher azureMatcher = SAS_TOKEN_PATTERN.matcher(message);

    if (azureMatcher.find()) {
      message = azureMatcher.replaceAll("$1=XXXX");
    }

    // GCS
    Matcher gcsMatcher = PRIVATE_KEY_PATTERN.matcher(message);
    if (gcsMatcher.find()) {
      message =
          gcsMatcher.replaceAll(
              "-----BEGIN PRIVATE KEY-----" // #pragma: allowlist secret
                  + "\\\\nXXXX\\\\n"
                  + "-----END PRIVATE KEY-----");
    }

    gcsMatcher = PRIVATE_KEY_DATA_PATTERN.matcher(message);
    if (gcsMatcher.find()) {
      message = gcsMatcher.replaceAll("\"privateKeyData\": \"XXXX\"");
    }

    return message;
  }

  /**
   * Filter encryption material that may be buried inside a JSON string.
   *
   * @param message the message text which may contain encryption material
   * @return Return filtered message
   */
  public static String filterEncryptionMaterial(String message) {
    Matcher matcher =
        ENCRYPTION_MATERIAL_PATTERN.matcher(
            message.length() <= MAX_LENGTH ? message : message.substring(0, MAX_LENGTH));

    if (matcher.find()) {
      return matcher.replaceAll("\"encryptionMaterial\" : ****");
    }
    return message;
  }

  public static JSONObject maskJsonObject(JSONObject json) {
    for (Map.Entry<String, Object> entry : json.entrySet()) {
      if (entry.getValue() instanceof String) {
        entry.setValue(maskSecrets((String) entry.getValue()));
      } else if (entry.getValue() instanceof JSONArray) {
        maskJsonArray((JSONArray) entry.getValue());
      } else if (entry.getValue() instanceof JSONObject) {
        maskJsonObject((JSONObject) entry.getValue());
      }
    }
    return json;
  }

  public static JSONArray maskJsonArray(JSONArray array) {
    for (int i = 0; i < array.size(); i++) {
      Object node = array.get(i);
      if (node instanceof JSONObject) {
        maskJsonObject((JSONObject) node);
      } else if (node instanceof JSONArray) {
        maskJsonArray((JSONArray) node);
      } else if (node instanceof String) {
        array.set(i, SecretDetector.maskSecrets((String) node));
      }
      // for other types, we can just leave it untouched
    }

    return array;
  }

  public static JsonNode maskJacksonNode(JsonNode node) {
    if (node.isTextual()) {
      String maskedText = SecretDetector.maskSecrets(node.textValue());
      if (!maskedText.equals(node.textValue())) {
        return new TextNode(maskedText);
      }
    } else if (node.isObject()) {
      ObjectNode objNode = (ObjectNode) node;
      Iterator<String> fieldNames = objNode.fieldNames();

      while (fieldNames.hasNext()) {
        String fieldName = fieldNames.next();
        JsonNode tmpNode = maskJacksonNode(objNode.get(fieldName));
        if (objNode.get(fieldName).isTextual()) {
          objNode.set(fieldName, tmpNode);
        }
      }
    } else if (node.isArray()) {
      ArrayNode arrayNode = (ArrayNode) node;
      for (int i = 0; i < arrayNode.size(); i++) {
        JsonNode tmpNode = maskJacksonNode(arrayNode.get(i));
        if (arrayNode.get(i).isTextual()) {
          arrayNode.set(i, tmpNode);
        }
      }
    }
    return node;
  }

  // This class aims to parse minidev.json's node better
  public static class SecretDetectorJSONStyle extends JSONStyle {
    public SecretDetectorJSONStyle() {
      super();
    }

    public void objectNext(Appendable out) throws IOException {
      out.append(", ");
    }

    public void arrayStop(Appendable out) throws IOException {
      out.append("] ");
    }

    public void arrayNextElm(Appendable out) throws IOException {
      out.append(", ");
    }
  }
}
