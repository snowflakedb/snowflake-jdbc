/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.util;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Search for credentials in a sql text
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

  private static final Pattern AWS_KEY_PATTERN = Pattern.compile(
      "(aws_key_id)|(aws_secret_key)|(access_key_id)|(secret_access_key)",
      Pattern.CASE_INSENSITIVE);

  // Used for detecting tokens in serialized JSON
  private static final Pattern AWS_TOKEN_PATTERN = Pattern.compile(
      "(accessToken|tempToken|keySecret)\"\\s*:\\s*\"([a-z0-9/+]{32,}={0,2})\"",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern SAS_TOKEN_PATTERN = Pattern.compile(
      "sig=([a-z0-9%]{32,})", Pattern.CASE_INSENSITIVE);

  private static final int LOOK_AHEAD = 10;

  // only attempt to find secrets in its leading 100Kb SNOW-30961
  private static final int MAX_LENGTH = 100 * 1000;

  private static final SFLogger LOGGER = SFLoggerFactory.getLogger(
      SecretDetector.class);

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
    // log before and after in case this is causing StackOverflowError
    LOGGER.debug("pre-regex getAWSSecretPos");

    Matcher matcher = AWS_KEY_PATTERN.matcher(text);

    ArrayList<SecretRange> awsSecretRanges = new ArrayList<>();

    while (matcher.find())
    {
      int beginPos = Math.min(matcher.end() + LOOK_AHEAD, text.length());

      while (beginPos > 0 && beginPos < text.length() &&
             isBase64(text.charAt(beginPos)))
      {
        beginPos--;
      }

      int endPos = Math.min(matcher.end() + LOOK_AHEAD, text.length());

      while (endPos < text.length() && isBase64(text.charAt(endPos)))
      {
        endPos++;
      }

      if (beginPos < text.length() && endPos <= text.length()
          && beginPos >= 0 && endPos >= 0)
      {
        awsSecretRanges.add(new SecretRange(beginPos + 1, endPos));
      }
    }

    LOGGER.debug("post-regex getAWSSecretPos");

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

  private static boolean isBase64(char ch)
  {
    return ('A' <= ch && ch <= 'Z')
           || ('a' <= ch && ch <= 'z')
           || ('0' <= ch && ch <= '9')
           || ch == '+'
           || ch == '/';
  }

  /**
   * mask AWS secret in the input string
   *
   * @param sql
   * @return masked string
   */
  public static String maskAWSSecret(String sql)
  {
    List<SecretDetector.SecretRange> secretRanges =
        SecretDetector.getAWSSecretPos(sql);
    for (SecretDetector.SecretRange secretRange : secretRanges)
    {
      sql = maskText(sql, secretRange.beginPos, secretRange.endPos);
    }
    return sql;
  }

  /**
   * Masks given text between begin position and end position.
   *
   * @param text   text to mask
   * @param begPos begin position (inclusive)
   * @param endPos end position (exclusive)
   * @return masked text
   */
  private static String maskText(String text, int begPos, int endPos)
  {
    // Convert the SQL statement to a char array to obe able to modify it.
    char[] chars = text.toCharArray();

    // Mask the value in the SQL statement using *.
    for (int curPos = begPos; curPos < endPos; curPos++)
    {
      chars[curPos] = '☺';
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
}
