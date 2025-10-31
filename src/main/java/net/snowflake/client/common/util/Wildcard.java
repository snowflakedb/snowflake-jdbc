package net.snowflake.client.common.util;

import java.util.regex.Pattern;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * This class wraps wildcard related functions and regular expression.
 *
 * @author hyu
 */
@SnowflakeJdbcInternalApi
public class Wildcard {
  /** Any single character */
  private static char SINGLE_CHAR = '_';

  /** O or more number of any characters */
  private static char MULTIPLE_CHAR = '%';

  /** escaped string */
  private static char ESCAPED_CHAR = '\\';

  private static String NON_ESCAPED_SINGLE_CHAR_REGEX =
      String.format("(?<!\\%c)%c", ESCAPED_CHAR, SINGLE_CHAR);

  private static String NON_ESCAPED_MULTIPLE_CHAR_REGEX =
      String.format("(?<!\\%c)%c", ESCAPED_CHAR, MULTIPLE_CHAR);

  private static String WILDCARD_CHARS_REGEX =
      String.format("(%s|%s)", NON_ESCAPED_SINGLE_CHAR_REGEX, NON_ESCAPED_MULTIPLE_CHAR_REGEX);
  /**
   * This is a regex to match whether a given string a wildcard pattern or not It means match a
   * string that contains _ but not \_ or % but not \%
   */
  private static String WILDCARD_STRING_REGEX = String.format(".*%s.*", WILDCARD_CHARS_REGEX);

  private static Pattern WILDCARD_STRING_PATTERN = Pattern.compile(WILDCARD_STRING_REGEX);

  /**
   * Convert a wildcard pattern string to a java regex pattern string. The convert way is as follow:
   * 1. Separate the string to several blocks of substring by delimiter % or _ (but not escaped) 2.
   * For each of block substring, remove the escaped string '\' and then quote substring (Quote
   * means everything within quote is treated as literal, not java regex) 3. For every '%' convert
   * to '.*' and for every '_' convert to '.' 4. Append all of them into one final string
   *
   * @param wildcardPattern a SQL wild card pattern
   * @return regex pattern
   */
  public static String toRegexStr(String wildcardPattern) {
    Pattern separator = Pattern.compile(WILDCARD_CHARS_REGEX);

    String[] nonWildCardCharBlocks = separator.split(wildcardPattern);

    StringBuilder regexBuilder = new StringBuilder();
    int currentIndex = 0;
    for (String block : nonWildCardCharBlocks) {
      int len = block.length();
      if (len > 0) {
        String strippedBlock = stripEscapedChar(block);
        regexBuilder.append(Pattern.quote(strippedBlock));
      }

      currentIndex += len;
      if (currentIndex < wildcardPattern.length()) {
        char charToConvert = wildcardPattern.charAt(currentIndex);
        assert charToConvert == SINGLE_CHAR || charToConvert == MULTIPLE_CHAR;

        regexBuilder.append((charToConvert == SINGLE_CHAR) ? "." : ".*");
        currentIndex++;
      }
    }

    /* Deal with the special case that "abc" and "abc_" will both return only one
     * String by Pattern.split method (separator.split("abc_") will not return ["abc", ""])
     * But interestingly, separator.split("_abc") will return ["", "abc"]
     *
     * Also, separator.split("%%_") will return empty array.
     */
    while (currentIndex < wildcardPattern.length()) {
      char charToConvert = wildcardPattern.charAt(currentIndex);
      assert charToConvert == SINGLE_CHAR || charToConvert == MULTIPLE_CHAR;
      regexBuilder.append((charToConvert == SINGLE_CHAR) ? "." : ".*");
      currentIndex++;
    }
    return regexBuilder.toString();
  }

  /**
   * Converts a wild card pattern to regex
   *
   * @param wildcardPattern a wild card pattern
   * @param isCaseSensitive is case insensitive?
   * @return a regex pattern
   */
  public static Pattern toRegexPattern(String wildcardPattern, boolean isCaseSensitive) {
    if (wildcardPattern == null) {
      return null;
    }

    return isCaseSensitive
        ? Pattern.compile(toRegexStr(wildcardPattern))
        : Pattern.compile(toRegexStr(wildcardPattern), Pattern.CASE_INSENSITIVE);
  }

  /**
   * Used to determine if the input string is wildcard pattern By wildcard pattern, we means a
   * string contains '%' but not '\%' or '_' but not '\_'
   *
   * @param inputString a string
   * @return true if the string includes a wild card pattern
   */
  public static boolean isWildcardPatternStr(String inputString) {
    if (inputString == null) {
      return false;
    }
    return WILDCARD_STRING_PATTERN.matcher(inputString).matches();
  }

  /**
   * A helper function within toRegexStr(String) method. For each substring, remove the escaped char
   * '\'
   *
   * @param inputString a string
   * @return a string without escaped chars
   */
  private static String stripEscapedChar(String inputString) {
    StringBuilder sb = new StringBuilder();
    boolean escaped = false;
    for (int i = 0; i < inputString.length(); i++) {
      if (inputString.charAt(i) == ESCAPED_CHAR && !escaped) {
        escaped = true;
      } else {
        sb.append(inputString.charAt(i));
        escaped = false;
      }
    }
    return sb.toString();
  }
}
