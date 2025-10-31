package net.snowflake.client.common.core;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import net.snowflake.client.common.util.TimeUtil;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * @author jhuang
 */
@SnowflakeJdbcInternalApi
public class SnowflakeDateTimeFormat {
  private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

  public enum ElementType {
    Year2digit_ElementType("YY", "yy"),
    Year_ElementType("YYYY", "yyyy"),
    Month_ElementType("MM", "MM"),
    MonthAbbrev_ElementType("MON", "MMM"),
    MonthFullName_ElementType("MMMM", "MMMM"),
    DayOfMonth_ElementType("DD", "dd"),
    DayOfWeekAbbrev_ElementType("DY", "EEE"),
    Hour24_ElementType("HH24", "HH"),
    Hour12_ElementType("HH12", "hh"),
    Hour_ElementType("HH", "HH"),
    Ante_Meridiem_ElementType("AM", "a"),
    Post_Meridiem_ElementType("PM", "a"),
    Minute_ElementType("MI", "mm"),
    Second_ElementType("SS", "ss"),
    MilliSecond_ElementType("FF", ""), // special code for parsing fractions
    TZOffsetHourColonMin_ElementType("TZH:TZM", "XXX"),
    TZOffsetHourMin_ElementType("TZHTZM", "XX"),
    TZOffsetHourOnly_ElementType("TZH", "X"),
    TZAbbr_ElementType("TZD", "z");

    private final String sqlFormat;
    private final String javaFormat; // java SimpleDateFormat

    /**
     * Constructor for ElementType
     *
     * @param sqlFormat
     * @param javaFormat
     */
    ElementType(String sqlFormat, String javaFormat) {
      this.sqlFormat = sqlFormat;
      this.javaFormat = javaFormat;
    }

    public String getSqlFormat() {
      return sqlFormat;
    }

    public String getJavaFormat() {
      return javaFormat;
    }
  }

  private static class Fragment {
    private final String javaFormat;

    Fragment(String javaFormat) {
      this.javaFormat = javaFormat;
    }
  }

  /** Our SQL format */
  private final String sqlFormat;

  private final boolean isDeprecated;

  /** If set, we'll use auto-logic during parsing */
  private boolean automaticParsing;

  /** Enables auto-scaling of Epoch time */
  public boolean epochAutoScale = true;

  private List<Fragment> fragments;
  private SimpleDateFormat simpleDateFormat;

  // Precision of the fractions. If -1, type-based.
  private int fractionsLen = -1;
  // Position of the fractions in the format. If -1, fractions are absent.
  private int fractionsPos = -1;
  // Defines if fractions are prefixed with a dot
  private boolean fractionsWithDot = false;
  // Formatter used to parse everything before fractions to find their place
  private SimpleDateFormat fractionsPreFormatter;

  // Formatter used to parse everything before the timezone to find its place.
  // If null, we know it's absent.
  private SimpleDateFormat timezonePreFormatter;
  // If we have timezones, what is their format.
  private ElementType timezoneElementType;

  // If we have a 2-year year
  private boolean has2digitYear = false;

  // Bitmasks to use in type definition
  public static final int DATE = 1;
  public static final int TIME = 2;
  public static final int TIMESTAMP = 4;
  public static final int ANY_TYPE = DATE | TIME | TIMESTAMP;

  private final int type;

  // Array of all supported formats, to be used in AUTO parsing.
  // NOTE: generated with gen_date_formats.py, do not edit manually.
  // Should to be in sync with the CPP version
  private static final SnowflakeDateTimeFormat[] acceptedFormats = {
    // ISO_DATE_T_HOUR24_MINUTE_SECOND_FRAC_TZHM
    // Ex: "2013-04-28T20:57:01.123456789+07:00"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_MINUTE_SECOND_FRAC_TZHCM
    // Ex: "2013-04-28 20:57:01.123456789+07:00"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24:MI:SS.FFTZH:TZM", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_MINUTE_SECOND_FRAC_TZH
    // Ex: "2013-04-28 20:57:01.123456789+07"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24:MI:SS.FFTZH", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_MINUTE_SECOND_FRAC_TZSHCM
    // Ex: "2013-04-28 20:57:01.123456789 +07:00"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24:MI:SS.FF TZH:TZM", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_MINUTE_SECOND_FRAC_TZSHM
    // Ex: "2013-04-28 20:57:01.123456789 +0700"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24:MI:SS.FF TZHTZM", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_MINUTE_SECOND_TZSHCM
    // Ex: "2013-04-28 20:57:01 +07:00"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24:MI:SS TZH:TZM", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_MINUTE_SECOND_TZSHM
    // Ex: "2013-04-28 20:57:01 +0700"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24:MI:SS TZHTZM", TIMESTAMP | DATE),

    // ISO_DATE_T_HOUR24_MINUTE_SECOND_FRAC
    // Ex: "2013-04-28T20:57:01.123456"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD\"T\"HH24:MI:SS.FF", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_MINUTE_SECOND_FRAC2
    // Ex: "2013-04-28 20:57:01.123456"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24:MI:SS.FF", TIMESTAMP | DATE),

    // ISO_DATE_T_HOUR24_MINUTE_SECOND
    // Ex: "2013-04-28T20:57:01"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD\"T\"HH24:MI:SS", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_MINUTE_SECOND2
    // Ex: "2013-04-28 20:57:01"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24:MI:SS", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_MINUTE
    // Ex: "2013-04-28T20:57"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD\"T\"HH24:MI", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_MINUTE2
    // Ex: "2013-04-28 20:57"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24:MI", TIMESTAMP | DATE),

    // ISO_DATE_T_HOUR24
    // Ex: "2013-04-28T20"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD\"T\"HH24", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_2
    // Ex: "2013-04-28 20"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24", TIMESTAMP | DATE),

    // ISO_DATE
    // Ex: "2013-04-28"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD", TIMESTAMP | DATE),

    // ISO_US_SIMPLE_DATE
    // Ex: "17-DEC-1980"
    SnowflakeDateTimeFormat.fromSqlFormat("DD-MON-YYYY", TIMESTAMP | DATE),

    // ISO_DATE_T_HOUR24_MINUTE_SECOND_TZHCM
    // Ex: "2013-04-28T20:57:01-07:00"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD\"T\"HH24:MI:SSTZH:TZM", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_MINUTE_SECOND_TZHCM
    // Ex: "2013-04-28 20:57:01-07:00"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24:MI:SSTZH:TZM", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_MINUTE_SECOND_TZH
    // Ex: "2013-04-28 20:57:01-07"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24:MI:SSTZH", TIMESTAMP | DATE),

    // ISO_DATE_T_HOUR24_MINUTE_TZHCM
    // Ex: "2013-04-28T20:57+07:00"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD\"T\"HH24:MITZH:TZM", TIMESTAMP | DATE),

    // ISO_DATE_HOUR24_MINUTE_TZHCM
    // Ex: "2013-04-28 20:57+07:00"
    SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24:MITZH:TZM", TIMESTAMP | DATE),

    // RFC_DATE_HOUR24_MINUTE_SECOND_TZ
    // Ex: "Thu, 21 Dec 2000 16:01:07 +0200"
    SnowflakeDateTimeFormat.fromSqlFormat("DY, DD MON YYYY HH24:MI:SS TZHTZM", TIMESTAMP | DATE),

    // RFC_DATE_HOUR24_MINUTE_SECOND_FRAC_TZ
    // Ex: "Thu, 21 Dec 2000 16:01:07.123456 +0200"
    SnowflakeDateTimeFormat.fromSqlFormat("DY, DD MON YYYY HH24:MI:SS.FF TZHTZM", TIMESTAMP | DATE),

    // RFC_DATE_HOUR12_MINUTE_SECOND_MERIDIEM_TZ
    // Ex: "Thu, 21 Dec 2000 04:01:07 PM +0200"
    SnowflakeDateTimeFormat.fromSqlFormat("DY, DD MON YYYY HH12:MI:SS AM TZHTZM", TIMESTAMP | DATE),

    // RFC_DATE_HOUR12_MINUTE_SECOND_FRAC_MERIDIEM_TZ
    // Ex: "Thu, 21 Dec 2000 04:01:07.123456 PM +0200"
    SnowflakeDateTimeFormat.fromSqlFormat(
        "DY, DD MON YYYY HH12:MI:SS.FF AM TZHTZM", TIMESTAMP | DATE),

    // RFC_DATE_HOUR24_MINUTE_SECOND
    // Ex: "Thu, 21 Dec 2000 16:01:07"
    SnowflakeDateTimeFormat.fromSqlFormat("DY, DD MON YYYY HH24:MI:SS", TIMESTAMP | DATE),

    // RFC_DATE_HOUR24_MINUTE_SECOND_FRAC
    // Ex: "Thu, 21 Dec 2000 16:01:07.123456"
    SnowflakeDateTimeFormat.fromSqlFormat("DY, DD MON YYYY HH24:MI:SS.FF", TIMESTAMP | DATE),

    // RFC_DATE_HOUR12_MINUTE_SECOND_MERIDIEM
    // Ex: "Thu, 21 Dec 2000 04:01:07 PM"
    SnowflakeDateTimeFormat.fromSqlFormat("DY, DD MON YYYY HH12:MI:SS AM", TIMESTAMP | DATE),

    // RFC_DATE_HOUR12_MINUTE_SECOND_FRAC_MERIDIEM
    // Ex: "Thu, 21 Dec 2000 04:01:07.123456 PM"
    SnowflakeDateTimeFormat.fromSqlFormat("DY, DD MON YYYY HH12:MI:SS.FF AM", TIMESTAMP | DATE),

    // TWITTER_DATE_HOUR24_MIN_SEC_TZ_YEAR
    // Twitter timestamp format. Ex: Mon Jul 08 18:09:51 +0000 2013
    SnowflakeDateTimeFormat.fromSqlFormat("DY MON DD HH24:MI:SS TZHTZM YYYY", TIMESTAMP | DATE),

    // ISO_HOUR24_MINUTE_SECOND_FRAC_TZ
    // Ex: "20:57:01.123456789+07:00"
    SnowflakeDateTimeFormat.fromSqlFormat("HH24:MI:SS.FFTZH:TZM", TIME),

    // ISO_HOUR24_MINUTE_SECOND_FRAC
    // Ex: "20:57:01.123456789"
    SnowflakeDateTimeFormat.fromSqlFormat("HH24:MI:SS.FF", TIME),

    // RFC_HOUR12_MINUTE_SECOND_FRAC_MERIDIEM
    // Ex: "07:57:01.123456789 PM"
    SnowflakeDateTimeFormat.fromSqlFormat("HH12:MI:SS.FF AM", TIME),

    // ISO_HOUR24_MINUTE_SECOND
    // Ex: "20:57:01"
    SnowflakeDateTimeFormat.fromSqlFormat("HH24:MI:SS", TIME),

    // RFC_HOUR12_MINUTE_SECOND_MERIDIEM
    // Ex: "04:01:07 PM"
    SnowflakeDateTimeFormat.fromSqlFormat("HH12:MI:SS AM", TIME),

    // ISO_HOUR24_MINUTE
    // Ex: "20:57"
    SnowflakeDateTimeFormat.fromSqlFormat("HH24:MI", TIME),

    // RFC_HOUR12_MINUTE_MERIDIEM
    // Ex: "04:01 PM"
    SnowflakeDateTimeFormat.fromSqlFormat("HH12:MI AM", TIME),

    // ISO_US_DATE_ALT1
    // Ex: "12/17/1980"
    SnowflakeDateTimeFormat.deprecatedFormat("MM/DD/YYYY", TIMESTAMP | DATE),

    // ALT_DATE_HOUR24_MIN_SEC
    // Ex: "2/18/2008 02:36:48"
    SnowflakeDateTimeFormat.deprecatedFormat("MM/DD/YYYY HH24:MI:SS", TIMESTAMP | DATE),
  };

  public static SnowflakeDateTimeFormat fromSqlFormat(String sqlFormat) {
    return new SnowflakeDateTimeFormat(sqlFormat, ANY_TYPE, false);
  }

  private static SnowflakeDateTimeFormat fromSqlFormat(String sqlFormat, int type) {
    return new SnowflakeDateTimeFormat(sqlFormat, type, false);
  }

  private static SnowflakeDateTimeFormat deprecatedFormat(String sqlFormat, int type) {
    return new SnowflakeDateTimeFormat(sqlFormat, type, true);
  }

  private SnowflakeDateTimeFormat(String sqlFormat, int type, boolean isDeprecated) {
    // limit sql format length
    if (sqlFormat.length() > 1024) {
      throw new IllegalArgumentException("timestamp format too long");
    }

    this.sqlFormat = sqlFormat;
    this.type = type;
    this.isDeprecated = isDeprecated;
    fragments = new ArrayList<>();
    if (sqlFormat.compareToIgnoreCase("auto") == 0) {
      automaticParsing = true;
    } else {
      automaticParsing = false;
      compile(sqlFormat);
      assert fragments.size() <= 1;
      simpleDateFormat = new SimpleDateFormat(toSimpleDateTimePattern());
    }
  }

  public String getSqlFormat() {
    return sqlFormat;
  }

  private void createNewFragment(String javaTimestampFormat) {
    fragments.add(new Fragment(javaTimestampFormat));
  }

  /**
   * Add the java format for the element to the java format string builder Add the element to the
   * list of elements.
   *
   * @param element
   * @param javaTimestampFormat
   * @param elementTypes
   * @return Return the length of the sql format corresponding to the element.
   */
  private int addElement(
      ElementType element, StringBuilder javaTimestampFormat, List<ElementType> elementTypes) {
    javaTimestampFormat.append(element.getJavaFormat());
    elementTypes.add(element);
    return element.getSqlFormat().length();
  }

  /**
   * Adds a raw character to a string format by quoting it
   *
   * @param stringFormat
   * @param charToAdd
   */
  private void addRawChar(StringBuilder stringFormat, char charToAdd) {
    int curSize = stringFormat.length();
    if (charToAdd == '\'') {
      // Special code for "'"
      stringFormat.append("''");
    } else if (curSize > 2
        && stringFormat.charAt(curSize - 1) == '\''
        && stringFormat.charAt(curSize - 2) != '\'') {
      // Previous character was "raw", combine them
      if (fractionsPos == curSize) {
        // We're deleting a character before fractions, need to adjust for that
        fractionsPos--;
      }
      stringFormat.deleteCharAt(curSize - 1);
      stringFormat.append(charToAdd).append('\'');
    } else {
      stringFormat.append('\'').append(charToAdd).append('\'');
    }
  }

  /**
   * A function to parse SQL timestamp format and generate timestamp fragments.
   *
   * @param sqlTimestampFormat
   */
  private void compile(String sqlTimestampFormat) {
    StringBuilder javaTimestampFormat = new StringBuilder();
    List<ElementType> elementTypes = new ArrayList<>();

    int idx = 0;

    String formatUpperCase = sqlTimestampFormat.toUpperCase();

    while (idx < formatUpperCase.length()) {
      switch (formatUpperCase.charAt(idx)) {
        case 'A':
          if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.Ante_Meridiem_ElementType.getSqlFormat())) {
            idx +=
                addElement(
                    ElementType.Ante_Meridiem_ElementType, javaTimestampFormat, elementTypes);
          } else {
            addRawChar(javaTimestampFormat, sqlTimestampFormat.charAt(idx++));
          }
          break;

        case 'D':
          if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.DayOfMonth_ElementType.getSqlFormat())) {
            idx +=
                addElement(ElementType.DayOfMonth_ElementType, javaTimestampFormat, elementTypes);
          } else if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.DayOfWeekAbbrev_ElementType.getSqlFormat())) {
            idx +=
                addElement(
                    ElementType.DayOfWeekAbbrev_ElementType, javaTimestampFormat, elementTypes);
          } else {
            addRawChar(javaTimestampFormat, sqlTimestampFormat.charAt(idx++));
          }
          break;

        case 'H':
          if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.Hour24_ElementType.getSqlFormat())) {
            idx += addElement(ElementType.Hour24_ElementType, javaTimestampFormat, elementTypes);
          } else if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.Hour12_ElementType.getSqlFormat())) {
            idx += addElement(ElementType.Hour12_ElementType, javaTimestampFormat, elementTypes);
          } else if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.Hour_ElementType.getSqlFormat())) {
            idx += addElement(ElementType.Hour_ElementType, javaTimestampFormat, elementTypes);
          } else {
            addRawChar(javaTimestampFormat, sqlTimestampFormat.charAt(idx++));
          }
          break;

        case 'M':
          if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.MonthFullName_ElementType.getSqlFormat())) {
            idx +=
                addElement(
                    ElementType.MonthFullName_ElementType, javaTimestampFormat, elementTypes);
          } else if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.Month_ElementType.getSqlFormat())) {
            idx += addElement(ElementType.Month_ElementType, javaTimestampFormat, elementTypes);
          } else if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.Minute_ElementType.getSqlFormat())) {
            idx += addElement(ElementType.Minute_ElementType, javaTimestampFormat, elementTypes);
          } else if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.MonthAbbrev_ElementType.getSqlFormat())) {
            idx +=
                addElement(ElementType.MonthAbbrev_ElementType, javaTimestampFormat, elementTypes);
          } else {
            addRawChar(javaTimestampFormat, sqlTimestampFormat.charAt(idx++));
          }
          break;

        case 'P':
          if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.Post_Meridiem_ElementType.getSqlFormat())) {
            idx +=
                addElement(
                    ElementType.Post_Meridiem_ElementType, javaTimestampFormat, elementTypes);
          } else {
            addRawChar(javaTimestampFormat, sqlTimestampFormat.charAt(idx++));
          }
          break;

        case 'S':
          if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.Second_ElementType.getSqlFormat())) {
            idx += addElement(ElementType.Second_ElementType, javaTimestampFormat, elementTypes);
          } else {
            addRawChar(javaTimestampFormat, sqlTimestampFormat.charAt(idx++));
          }
          break;

        case 'T':
          {
            if (formatUpperCase
                .substring(idx)
                .startsWith(ElementType.TZOffsetHourColonMin_ElementType.getSqlFormat())) {
              timezonePreFormatter = new SimpleDateFormat(javaTimestampFormat.toString());
              timezoneElementType = ElementType.TZOffsetHourColonMin_ElementType;
              idx +=
                  addElement(
                      ElementType.TZOffsetHourColonMin_ElementType,
                      javaTimestampFormat,
                      elementTypes);
            } else if (formatUpperCase
                .substring(idx)
                .startsWith(ElementType.TZOffsetHourMin_ElementType.getSqlFormat())) {
              timezonePreFormatter = new SimpleDateFormat(javaTimestampFormat.toString());
              timezoneElementType = ElementType.TZOffsetHourMin_ElementType;
              idx +=
                  addElement(
                      ElementType.TZOffsetHourMin_ElementType, javaTimestampFormat, elementTypes);
            } else if (formatUpperCase
                .substring(idx)
                .startsWith(ElementType.TZOffsetHourOnly_ElementType.getSqlFormat())) {
              timezonePreFormatter = new SimpleDateFormat(javaTimestampFormat.toString());
              timezoneElementType = ElementType.TZOffsetHourOnly_ElementType;
              idx +=
                  addElement(
                      ElementType.TZOffsetHourOnly_ElementType, javaTimestampFormat, elementTypes);
            } else if (formatUpperCase
                .substring(idx)
                .startsWith(ElementType.TZAbbr_ElementType.getSqlFormat())) {
              timezonePreFormatter = new SimpleDateFormat(javaTimestampFormat.toString());
              timezoneElementType = ElementType.TZAbbr_ElementType;
              idx += addElement(ElementType.TZAbbr_ElementType, javaTimestampFormat, elementTypes);
            } else {
              addRawChar(javaTimestampFormat, sqlTimestampFormat.charAt(idx++));
            }
          }
          break;

        case 'Y':
          // It's important 4-digit year goes before 2-digit year
          if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.Year_ElementType.getSqlFormat())) {
            idx += addElement(ElementType.Year_ElementType, javaTimestampFormat, elementTypes);
          } else if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.Year2digit_ElementType.getSqlFormat())) {
            has2digitYear = true;
            idx +=
                addElement(ElementType.Year2digit_ElementType, javaTimestampFormat, elementTypes);
          } else {
            addRawChar(javaTimestampFormat, sqlTimestampFormat.charAt(idx++));
          }
          break;

        case '.':
          if (idx + 1 < formatUpperCase.length()
              && formatUpperCase
                  .substring(idx + 1)
                  .startsWith(ElementType.MilliSecond_ElementType.getSqlFormat())) {
            // Will be FF, just mark that there's a dot before FF
            fractionsWithDot = true;
            idx++;
          } else {
            addRawChar(javaTimestampFormat, sqlTimestampFormat.charAt(idx++));
          }
          break;

        case 'F':
          if (formatUpperCase
              .substring(idx)
              .startsWith(ElementType.MilliSecond_ElementType.getSqlFormat())) {
            idx += ElementType.MilliSecond_ElementType.getSqlFormat().length();
            // @todo Handle multiple occurences?
            // Construct formatter to find fractions position.
            fractionsPreFormatter = new SimpleDateFormat(javaTimestampFormat.toString());
            // Save fractions information
            fractionsPos = javaTimestampFormat.toString().length();
            fractionsLen = -1;
            // Check if FF is followed by the length specification (e.g. FF3)
            if (idx < formatUpperCase.length() && Character.isDigit(formatUpperCase.charAt(idx))) {
              fractionsLen = Character.digit(formatUpperCase.charAt(idx), 10);
              idx++;
            }
          } else {
            addRawChar(javaTimestampFormat, sqlTimestampFormat.charAt(idx++));
          }
          break;

        case '\"':
          // two double quotes become a single double quote;
          // in all other cases, replace double quotes with single quotes;
          // single quotes are Java's way of quoting things in a datetime format string

          int endIdx = idx + 1;

          while (endIdx < sqlTimestampFormat.length()
              && sqlTimestampFormat.charAt(endIdx) != '\"') {
            endIdx++;
          }

          if (endIdx == sqlTimestampFormat.length()) {
            throw new IllegalArgumentException("Unterminated '\"'");
          }

          if (endIdx == idx + 1) {
            // two double quotes = a single double quote
            javaTimestampFormat.append("\"");
          } else {
            // replace double quote with a single quote
            javaTimestampFormat.append("'");
            javaTimestampFormat.append(sqlTimestampFormat, idx + 1, endIdx);
            javaTimestampFormat.append("'");
          }

          idx = endIdx + 1;

          break;

        default:
          addRawChar(javaTimestampFormat, sqlTimestampFormat.charAt(idx++));
          break;
      }
    }

    if (!elementTypes.isEmpty() || javaTimestampFormat.length() > 0 || fractionsLen > 0) {
      createNewFragment(javaTimestampFormat.toString());
    }
  }

  public final String toSimpleDateTimePattern() {
    if (automaticParsing) {
      // This is only supposed to happen in logging.
      return "AUTO";
    }

    if (fragments.size() == 0) {
      return "";
    } else if (fragments.size() == 1) {
      return fragments.get(0).javaFormat;
    } else {
      return fragments.get(0).javaFormat + "FFF" + fragments.get(1).javaFormat;
    }
  }

  public String format(Timestamp timestamp, String timeZoneId, int scale) {
    return format(timestamp, (timeZoneId == null) ? GMT : TimeZone.getTimeZone(timeZoneId), scale);
  }

  public String format(Timestamp timestamp, TimeZone timeZone, int scale) {
    return format(timestamp, timeZone, timestamp.getNanos(), scale);
  }

  public String format(java.util.Date date, String timeZoneId) {
    return format(date, (timeZoneId == null) ? GMT : TimeZone.getTimeZone(timeZoneId));
  }

  public String format(java.util.Date date, TimeZone timeZone) {
    return format(date, timeZone, 0, 0);
  }

  public String format(SFTime sfTime, int scale) {
    return format(
        new Time(sfTime.getFractionalSeconds(3)), GMT, sfTime.getNanosecondsWithinSecond(), scale);
  }

  // Private function performing actual formatting
  private String format(java.util.Date timestampOrDate, TimeZone timeZone, int nanos, int scale) {
    SimpleDateFormat formatter;

    if (fractionsPos >= 0) { // Construct a special formatter, with nanos embedded
      assert fragments.size() <= 1;
      if (fractionsLen >= 0) {
        scale = fractionsLen;
      }
      String nanoStr = String.format("%1$09d", nanos).substring(0, scale);
      if (fractionsWithDot) {
        nanoStr = "." + nanoStr;
      }
      String newDateFormat;
      if (fragments.size() > 0) {
        String oldFormat = this.fragments.get(0).javaFormat;
        newDateFormat =
            oldFormat.substring(0, fractionsPos) + nanoStr + oldFormat.substring(fractionsPos);
      } else {
        newDateFormat = nanoStr;
      }
      formatter = new SimpleDateFormat(newDateFormat);
    } else {
      if (simpleDateFormat == null) {
        throw new IllegalArgumentException(
            "formatter is null. automaticParsing: " + automaticParsing);
      }
      formatter = simpleDateFormat;
    }
    formatter.setCalendar(CalendarCache.get(timeZone));
    return formatter.format(timestampOrDate);
  }

  /**
   * @param stringToParse String to parse
   * @param timeZone TimeZone to perform parsing in
   * @param centuryBoundary If > 0, defines the century boundary
   * @param ignoreTimezone If the string contains a timezone, ignore it
   * @param isStrict If set, the string is parsed strictly
   * @param cCompatibility corrects incompatibilities between Java and C libraries for timestamps
   *     that are ambiguous (e.g. '2016-11-06 01:30') or illegal (e.g. '2016-03-13 02:30') due to
   *     DST rules.
   * @return Parsed timestamp, or null if can't parse.
   */
  private SFTimestamp tryParsing(
      String stringToParse,
      TimeZone timeZone,
      int centuryBoundary,
      boolean ignoreTimezone,
      boolean isStrict,
      boolean cCompatibility) {
    int nanos = 0;

    if (fractionsPos >= 0) { // Special logic for strings with fractions
      // First, parse the first fragment
      assert fractionsPreFormatter != null;
      ParsePosition pp = new ParsePosition(0);
      java.util.Date firstDate = fractionsPreFormatter.parse(stringToParse, pp);
      if (firstDate == null || pp.getIndex() == 0) {
        return null;
      }
      // Now, we try to parse fractional seconds
      int idx = pp.getIndex();
      int fracIdx = idx;
      boolean dotOK = true;
      if (fractionsWithDot) {
        // Check if there's a dot
        if (stringToParse.length() <= idx || stringToParse.charAt(idx) != '.') {
          dotOK = false;
        } else {
          idx++;
        }
      }
      int mul = 100 * 1000 * 1000; // counting in nanosecnods
      boolean hadDigits = false; // There was some digit to scan
      while (idx < stringToParse.length() && Character.isDigit(stringToParse.charAt(idx))) {
        if (mul > 0) {
          nanos += mul * Character.digit(stringToParse.charAt(idx), 10);
          mul /= 10;
        }
        idx++;
        hadDigits = true;
      }
      if (hadDigits && !dotOK) {
        return null;
      }
      // Now we have the fractions. Construct a new parser and a new
      // stringToParse and
      stringToParse = stringToParse.substring(0, fracIdx) + stringToParse.substring(idx);
    }
    if (simpleDateFormat == null) {
      throw new IllegalArgumentException(
          "formatter is null. automaticParsing: " + automaticParsing);
    }
    simpleDateFormat.setCalendar(CalendarCache.get(timeZone));
    // Don't make it lenient if we want strict parsing
    simpleDateFormat.setLenient(!isStrict);
    if (centuryBoundary > 0 && has2digitYear) {
      // Need to specify in which century we're working
      simpleDateFormat.set2DigitYearStart(
          java.sql.Date.valueOf(LocalDate.of(centuryBoundary, 1, 1)));
    }

    ParsePosition pp = new ParsePosition(0);
    java.util.Date date = simpleDateFormat.parse(stringToParse, pp);

    if (date == null || pp.getIndex() != stringToParse.length()) {
      return null;
    }

    // If we had a time zone, also parse it to get originating time zone
    if (timezonePreFormatter != null) {
      pp = new ParsePosition(0);
      timezonePreFormatter.parse(stringToParse, pp);
      int idx = pp.getIndex();
      // Parse timezone at index.
      // Apparently parsing just timezone gives the local-epoch date
      String inputString = stringToParse.substring(idx);
      String formatString = timezoneElementType.javaFormat;
      SimpleDateFormat tzFormatter = new SimpleDateFormat(formatString);
      pp = new ParsePosition(0);
      // This has to parse
      java.util.Date tzDate = tzFormatter.parse(inputString, pp);
      int timeZoneOffset = -(int) tzDate.getTime();
      if (ignoreTimezone) {
        // Need to adjust the moment in time, to remove the impact of the
        // timezone that influenced the current date value.
        long ms = date.getTime();
        ms += timeZoneOffset;
        date = new java.util.Date(ms);
      } else {
        // Construct the real source timezone and use it to generate output
        timeZone = new SimpleTimeZone(timeZoneOffset, "GENERATED" + timeZoneOffset);
      }
    }

    if (cCompatibility && !(timeZone instanceof TimeUtil.CCompatibleTimeZone)) {
      if (TimeUtil.isDSTAmbiguous(date.getTime(), timeZone)) {
        // For an ambiguous timestamp, such as '2016-11-06 01:30' in
        // America/Los_Angeles:
        // - C chooses the earlier instant, i.e.
        //   2016-11-06 01:30 -0700 or 2016-11-06 08:30 Z
        // - Java chooses the later instant, i.e.
        //   2016-11-06 01:30 -0800 or 2016-11-06 09:30 Z
        // For compatibility, we need to bring back the Java timestamp by an
        // hour.

        date = new java.util.Date(date.getTime() - timeZone.getDSTSavings());
      }

      // Note: We can't detect DST-illegal timestamps without reimplementing
      // SimpleDateFormat.
    }

    // Construct a Timestamp
    return SFTimestamp.fromDate(date, nanos, timeZone);
  }

  private boolean isUndesiredType(int type) {
    return (type & this.type) == 0;
  }

  /**
   * Interface used by the parser for GS interactions, including configurations and usage tracking.
   */
  public interface ParserGSInteractions {
    /**
     * @return Whether the string should be parsed strictly.
     */
    default boolean isStrictParsing() {
      return true;
    }

    /**
     * @return When using auto parsing, whether the string should be parsed if the type of the
     *     format doesn't match the desired type.
     */
    default boolean shouldRejectWrongType() {
      return true;
    }

    /**
     * @return Whether the parser should correct incompatibilities between Java and C libraries for
     *     timestamps that are ambiguous (e.g. '2016-11-06 01:30') or illegal (e.g. '2016-03-13
     *     02:30') due to time zone transitions.
     */
    default boolean isCCompatible() {
      return true;
    }

    /**
     * Should be called when a string is auto-parsed with a deprecated format.
     *
     * @param format The deprecated format used.
     * @return Whether the result should be accepted.
     */
    default boolean handleDeprecatedFormat(SnowflakeDateTimeFormat format) {
      return true;
    }

    /**
     * Should be called when the scale is guessed.
     *
     * @param scale The guessed scale.
     */
    default void handleGuessedScale(int scale) {}

    /**
     * Should be called when the type of the format doesn't match the desired type but not rejected.
     * Only used for auto parsing.
     */
    default void handleAcceptedUndesiredType() {}
  }

  private static final ParserGSInteractions defaultGsInteractions = new ParserGSInteractions() {};

  /**
   * Parses a string into an SFTimestamp.
   *
   * @param stringToParse String to parse.
   * @param timeZone Timezone to use if it's not found in the string. If null, GMT is used by
   *     default.
   * @param centuryBoundary If &gt; 0, defines the century boundary.
   * @param formatType Type(s) of the format(s) desired for auto parsing.
   * @param ignoreTimezone If the string contains a timezone, ignore it.
   * @param gsInteractions Object containing configuration and usage tracking for GS, or null if no
   *     special handling is needed.
   * @return Parsed SFTimestamp. Can be null if nothing parsed. If the string had a timezone which
   *     is not ignored, it will be set.
   */
  public SFTimestamp parse(
      String stringToParse,
      TimeZone timeZone,
      int centuryBoundary,
      int formatType,
      boolean ignoreTimezone,
      ParserGSInteractions gsInteractions) {
    if (gsInteractions == null) {
      gsInteractions = defaultGsInteractions;
    }

    timeZone = (timeZone == null) ? GMT : timeZone;

    final boolean isStrict = gsInteractions.isStrictParsing();
    final boolean cCompatibility = gsInteractions.isCCompatible();

    if (!automaticParsing) {
      return tryParsing(
          stringToParse, timeZone, centuryBoundary, ignoreTimezone, isStrict, cCompatibility);
    }

    SFTimestamp result = null;

    // First try parsing as an integer - this allows loading unix-epoch based
    // times
    try {
      // 1000 years in seconds
      long SECONDS_LIMIT_FOR_EPOCH = 31536000000L;
      long MILLISECONDS_LIMIT_FOR_EPOCH = SECONDS_LIMIT_FOR_EPOCH * 1000;
      long MICROSECONDS_LIMIT_FOR_EPOCH = SECONDS_LIMIT_FOR_EPOCH * 1000000;
      // Parse as long
      long val = Long.parseLong(stringToParse);
      if (!epochAutoScale) {
        return SFTimestamp.fromMilliseconds(val * 1000, GMT);
      } else {
        if (val > -SECONDS_LIMIT_FOR_EPOCH && val < SECONDS_LIMIT_FOR_EPOCH) {
          gsInteractions.handleGuessedScale(0);
          return SFTimestamp.fromMilliseconds(val * 1000, GMT);
        } else if (val > -MILLISECONDS_LIMIT_FOR_EPOCH && val < MILLISECONDS_LIMIT_FOR_EPOCH) {
          gsInteractions.handleGuessedScale(3);
          return SFTimestamp.fromMilliseconds(val, GMT);
        } else if (val > -MICROSECONDS_LIMIT_FOR_EPOCH && val < MICROSECONDS_LIMIT_FOR_EPOCH) {
          gsInteractions.handleGuessedScale(6);
          return SFTimestamp.fromNanoseconds(val * 1000, GMT);
        } else {
          gsInteractions.handleGuessedScale(9);
          return SFTimestamp.fromNanoseconds(val, GMT);
        }
      }
    } catch (NumberFormatException e) {
      // Parsing as long failed, do nothing, we'll try parsing as a date
    }

    // Try different date formats - use simpleDateFormat formatting
    for (SnowflakeDateTimeFormat format : acceptedFormats) {
      if (gsInteractions.shouldRejectWrongType() && format.isUndesiredType(formatType)) {
        // Skip formats with undesired type
        continue;
      }

      // We synchronize on format, as concurrent queries might be accessing
      // the same static format concurrently if "auto" setting is on.
      // Our formats do have a state that is modified during parsing, hence
      // we need to protect it to avoid corruption (see SNOW-4009).
      synchronized (format) {
        result =
            format.tryParsing(
                stringToParse, timeZone, centuryBoundary, ignoreTimezone, isStrict, cCompatibility);
      }

      if (result != null) {
        // If a format is deprecated, handle it with the handler. If the
        // handler doesn't accept it, then try other formats.
        if (format.isDeprecated && !gsInteractions.handleDeprecatedFormat(format)) {
          continue;
        }

        // Handle undesired format type
        if (format.isUndesiredType(formatType)) {
          gsInteractions.handleAcceptedUndesiredType();
        }

        break;
      }
    }

    return result;
  }

  public SFTimestamp parse(
      String stringToParse, TimeZone timeZone, int centuryBoundary, boolean ignoreTimezone) {
    return parse(stringToParse, timeZone, centuryBoundary, ANY_TYPE, ignoreTimezone, null);
  }

  public SFTimestamp parse(String stringToParse) {
    return parse(stringToParse, GMT, 0, false);
  }

  /**
   * Returns effective String format for TIMESTAMP_*_OUTPUT_FORMAT, assuming that if the specialized
   * format is empty, the generic format should be used.
   *
   * @param specialized Specialized format, e.g. TIMESTAMP_NTZ_OUTPUT_FORMAT
   * @param generic Generic format, usually TIMESTAMP_OUTPUT_FORMAT
   * @return Effective format
   */
  public static String effectiveSpecializedTimestampFormat(String specialized, String generic) {
    if (specialized == null || specialized.length() == 0) {
      return generic;
    }
    return specialized;
  }
}
