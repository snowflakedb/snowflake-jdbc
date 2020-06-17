/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.util;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Search for credentials in sql and/or other text
 */
public class SecretDetector
{
  // We look for long base64 encoded (and perhaps also URL encoded - hence the
  // '%' character in the regex) strings.
  // This will match some things that it shouldn't. The minimum number of
  // characters is essentially a random choice - long enough to not mask other
  // strings but not so long that it might miss things.
  private static final Pattern GENERIC_CREDS_PATTERN = Pattern.compile(
      "([a-z0-9+/%]{18,})", Pattern.CASE_INSENSITIVE);

  // "\\s*" refers to >= 0 spaces, "[^']" refers to chars other than `'`
  private static final Pattern AWS_KEY_PATTERN = Pattern.compile(
      "(aws_key_id|aws_secret_key|access_key_id|secret_access_key)\\s*=\\s*'(?<secret>[^']+)'",
      Pattern.CASE_INSENSITIVE);

  // Used for detecting tokens in serialized JSON
  private static final Pattern AWS_TOKEN_PATTERN = Pattern.compile(
      "(accessToken|tempToken|keySecret)\"\\s*:\\s*\"([a-z0-9/+]{32,}={0,2})\"",
      Pattern.CASE_INSENSITIVE);

  // Signature added in the query string of a URL in SAS based authentication
  // for S3 or Azure Storage requests
  private static final Pattern SAS_TOKEN_PATTERN = Pattern.compile(
      "(sig|signature|AWSAccessKeyId|password|passcode)=(?<secret>[a-z0-9%/+]{16,})",
      Pattern.CASE_INSENSITIVE);

  private static final Pattern PASSWORD_PATTERN = Pattern.compile(
      "(password|passcode|pwd)" +
      "([\'\"\\s:=]+)" +
      "(?<secret>[a-z0-9!\"#$%&'\\()*+,-./:;<=>?@\\[\\]^_`\\{|\\}~]{6,})",
      Pattern.CASE_INSENSITIVE);

  // "-----BEGIN PRIVATE KEY-----\n[PRIVATE-KEY]\n-----END PRIVATE KEY-----"
  private static final Pattern PRIVATE_KEY_PATTERN = Pattern.compile(
      "-----BEGIN PRIVATE KEY-----\\\\n([a-z0-9/+=\\\\n]{32,})\\\\n-----END PRIVATE KEY-----",
      Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

  // "privateKeyData": "[PRIVATE-KEY]"
  private static final Pattern PRIVATE_KEY_DATA_PATTERN = Pattern.compile(
      "\"privateKeyData\": \"([a-z0-9/+=\\\\n]{10,})\"",
      Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

  private static final Pattern CONNECTION_TOKEN_PATTERN = Pattern.compile(
      "(token|assertion content)" +
      "(['\"\\s:=]+)" +
      "(?<secret>[a-z0-9=/_\\-+]{8,})",
      Pattern.CASE_INSENSITIVE);

  private static final int LOOK_AHEAD = 10;

  // only attempt to find secrets in its leading 100Kb SNOW-30961
  private static final int MAX_LENGTH = 100 * 1000;

  private static final SFLogger LOGGER = SFLoggerFactory.getLogger(
      SecretDetector.class);

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
      };

  private static Set<String> SENSITIVE_NAME_SET = new HashSet<>(Arrays.asList(SENSITIVE_NAMES));

  /**
   * Check whether the name is sensitive
   *
   * @param name
   */
  public static boolean isSensitive(String name)
  {
    return SENSITIVE_NAME_SET.contains(name.toLowerCase());
  }

  /**
   * Determine whether a connection parameter should be masked if parameter values
   * are being printed. Sensitive parameters include passwords and token values.
   * Helper function for maskParameterValue().
   *
   * @param name of the parameter
   * @return true if the parameter should be masked
   */
  private static boolean isSensitiveParameter(String name)
  {
    Pattern PASSWORD_IN_NAME = Pattern.compile(".*?(password|pwd|token|proxyuser).*?", Pattern.CASE_INSENSITIVE);
    Matcher matcher = PASSWORD_IN_NAME.matcher(name);
    return isSensitive(name) || matcher.matches();
  }

  /**
   * Mask sensitive parameter values. Used currently for connection parameters
   * whose values are to be recorded for each session.
   *
   * @param key   parameter key
   * @param value parameter value, which is sometimes masked
   * @return the original value if the parameter key does not mark it as sensitive,
   * or return a masked text if the key is determined to be sensitive.
   */
  public static String maskParameterValue(String key, String value)
  {
    if (isSensitiveParameter(key))
    {
      return "☺☺☺☺☺";
    }
    return value;
  }

  /**
   * Find all the positions of aws key id and aws secret key.
   * The time complexity is O(n)
   *
   * @param text the sql text which may contain aws key
   * @return Return a list of begin/end positions of aws key id and
   * aws secret key.
   */
  private static List<SecretRange> getAWSSecretPos(String text)
  {
    Matcher matcher = AWS_KEY_PATTERN.matcher(
        text.length() <= MAX_LENGTH ? text : text.substring(0, MAX_LENGTH));

    ArrayList<SecretRange> awsSecretRanges = new ArrayList<>();

    while (matcher.find())
    {
      awsSecretRanges.add(new SecretRange(matcher.start("secret"), matcher.end("secret")));
    }

    return awsSecretRanges;
  }

  /**
   * Find all the positions of long base64 encoded strings that are typically
   * indicative of secrets or other keys.
   * The time complexity is O(n)
   *
   * @param text the sql text which may contain secrets
   * @return Return a list of begin/end positions of keys in the string
   */
  private static List<SecretRange> getGenericSecretPos(String text)
  {
    // log before and after in case this is causing StackOverflowError
    LOGGER.debug("pre-regex getGenericSecretPos");

    Matcher matcher = GENERIC_CREDS_PATTERN.matcher(
        text.length() <= MAX_LENGTH ? text : text.substring(0, MAX_LENGTH));

    ArrayList<SecretRange> awsSecretRanges = new ArrayList<>();

    while (matcher.find())
    {
      awsSecretRanges.add(new SecretRange(matcher.start(), matcher.end()));
    }

    LOGGER.debug("post-regex getGenericSecretPos");

    return awsSecretRanges;
  }

  /**
   * Finds positions of occurrences of all sensitive fields of SAS tokens in the
   * given text.
   *
   * @param text text which may contain SAS tokens
   * @return A list of begin/end positions of sensitive fields of SAS tokens
   */
  private static List<SecretRange> getSASTokenPos(String text)
  {

    Matcher matcher = SAS_TOKEN_PATTERN.matcher(
        text.length() <= MAX_LENGTH ? text : text.substring(0, MAX_LENGTH));

    List<SecretRange> secretRanges = new ArrayList<>();

    while (matcher.find())
    {
      // Gets begin/end position of only 'secret' group in the matched regex
      secretRanges.add(
          new SecretRange(
              matcher.start("secret"), matcher.end("secret")));
    }

    return secretRanges;
  }

  /**
   * Finds positions of occurrences of all password fields in the
   * given text.
   *
   * @param text text which may contain passwords
   * @return A list of begin/end positions of password fields
   */
  private static List<SecretRange> getPasswordPos(String text)
  {
    Matcher matcher = PASSWORD_PATTERN.matcher(
        text.length() <= MAX_LENGTH ? text : text.substring(0, MAX_LENGTH));

    List<SecretRange> secretRanges = new ArrayList<>();

    while (matcher.find())
    {
      secretRanges.add(new SecretRange(matcher.start("secret"), matcher.end("secret")));
    }

    return secretRanges;
  }

  /**
   * Finds positions of occurrences of all connection token fields in the
   * given text.
   *
   * @param text text which may contain connection token
   * @return A list of begin/end positions of connection token fields
   */
  private static List<SecretRange> getConnectionTokenPos(String text)
  {
    Matcher matcher = CONNECTION_TOKEN_PATTERN.matcher(
        text.length() <= MAX_LENGTH ? text : text.substring(0, MAX_LENGTH));

    List<SecretRange> secretRanges = new ArrayList<>();

    while (matcher.find())
    {
      secretRanges.add(new SecretRange(matcher.start("secret"), matcher.end("secret")));
    }

    return secretRanges;
  }

  private static boolean isBase64(char ch)
  {
    return ('A' <= ch && ch <= 'Z')
           || ('a' <= ch && ch <= 'z')
           || ('0' <= ch && ch <= '9')
           || ch == '+'
           || ch == '/'
           || ch == '=';
  }

  /**
   * mask AWS secret in the input string
   *
   * @param sql The sql text to mask
   * @return masked string
   */
  public static String maskAWSSecret(String sql)
  {
    List<SecretRange> secretRanges = SecretDetector.getAWSSecretPos(sql);

    return maskText(sql, secretRanges);
  }

  /**
   * Masks SAS token(s) in the input string
   *
   * @param text Text which may contain SAS token(s)
   * @return Masked string
   */
  public static String maskSASToken(String text)
  {
    List<SecretRange> secretRanges = SecretDetector.getSASTokenPos(text);

    return maskText(text, secretRanges);
  }

  /**
   * Masks any secrets present in the input string. This currently checks for
   * SAS tokens ({@link SecretDetector#maskSASToken(String)}) and AWS keys
   * ({@link SecretDetector#maskAWSSecret(String)}.
   *
   * @param text Text which may contain secrets
   * @return Masked string
   */
  public static String maskSecrets(String text)
  {
    List<SecretRange> secretRanges = SecretDetector.getAWSSecretPos(text);
    secretRanges.addAll(SecretDetector.getAWSSecretPos(text));
    secretRanges.addAll(SecretDetector.getSASTokenPos(text));
    secretRanges.addAll(SecretDetector.getPasswordPos(text));
    secretRanges.addAll(SecretDetector.getConnectionTokenPos(text));
    text = maskText(text, secretRanges);
    text = filterAccessTokens(text);
    return text;
  }

  /**
   * Masks given text between given list of begin/end positions.
   *
   * @param text   text to mask
   * @param ranges List of begin/end positions of text that need to be masked
   * @return masked text
   */
  private static String maskText(String text, List<SecretRange> ranges)
  {
    if (ranges.isEmpty())
    {
      return text;
    }

    // Convert the text to a char array to be able to modify it.
    char[] chars = text.toCharArray();

    for (SecretRange range : ranges)
    {
      int beginPos = range.beginPos;
      int endPos = range.endPos;

      for (int curPos = beginPos; curPos < endPos; curPos++)
      {
        chars[curPos] = '☺';
      }
    }

    // Convert it back to a string
    return String.valueOf(chars);
  }

  static class SecretRange
  {
    final int beginPos;
    final int endPos;

    SecretRange(int beginPos, int endPos)
    {
      this.beginPos = beginPos;
      this.endPos = endPos;
    }
  }

  /**
   * Filter access tokens that might be buried in JSON. Currently only used
   * to filter the scopedCreds passed for XP binary downloads
   *
   * @param message the message text which may contain secrets
   * @return Return filtered message
   */
  public static String filterAccessTokens(String message)
  {
    Matcher awsMatcher = AWS_TOKEN_PATTERN.matcher(message);

    // aws
    if (awsMatcher.find())
    {
      message = awsMatcher.replaceAll("$1\":\"XXXX\"");
    }

    // azure
    Matcher azureMatcher = SAS_TOKEN_PATTERN.matcher(message);

    if (azureMatcher.find())
    {
      message = azureMatcher.replaceAll("$1=XXXX");
    }

    // GCS
    Matcher gcsMatcher = PRIVATE_KEY_PATTERN.matcher(message);
    if (gcsMatcher.find())
    {
      message = gcsMatcher.replaceAll("-----BEGIN PRIVATE KEY-----\\\\nXXXX\\\\n-----END PRIVATE KEY-----");
    }

    gcsMatcher = PRIVATE_KEY_DATA_PATTERN.matcher(message);
    if (gcsMatcher.find())
    {
      message = gcsMatcher.replaceAll("\"privateKeyData\": \"XXXX\"");
    }

    return message;
  }

  public static JSONObject maskJsonObject(JSONObject json)
  {
    for (Map.Entry<String, Object> entry : json.entrySet())
    {
      if (entry.getValue() instanceof String)
      {
        entry.setValue(maskSecrets((String) entry.getValue()));
      }
      else if (entry.getValue() instanceof JSONArray)
      {
        maskJsonArray((JSONArray) entry.getValue());
      }
      else if (entry.getValue() instanceof JSONObject)
      {
        maskJsonObject((JSONObject) entry.getValue());
      }
    }
    return json;
  }

  public static JSONArray maskJsonArray(JSONArray array)
  {
    for (int i = 0; i < array.size(); i++)
    {
      Object node = array.get(i);
      if (node instanceof JSONObject)
      {
        maskJsonObject((JSONObject) node);
      }
      else if (node instanceof JSONArray)
      {
        maskJsonArray((JSONArray) node);
      }
      else if (node instanceof String)
      {
        array.set(i, SecretDetector.maskSecrets((String) node));
      }
      // for other types, we can just leave it untouched
    }

    return array;
  }
}
