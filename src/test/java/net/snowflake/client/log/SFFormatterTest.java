package net.snowflake.client.log;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SFFormatterTest {
  // Change these numbers if necessary
  /** The maximum time difference in millisecond allowed for timestamp and now */
  private static final long TIME_DIFFERENCE_BOUNDARY = 600000; // 10 minutes
  /** Number of iterations for the stress test */
  private static final int STRESS_TEST_ITERATION = 2000;

  /** Log record generator */
  private LRGenerator recordGenerator;

  @BeforeEach
  public void setUp() {
    recordGenerator = new LRGenerator(SFFormatter.CLASS_NAME_PREFIX + "TestClass", "TestMethod");
    recordGenerator.setFormatter(new SFFormatter());
  }

  /**
   * This test intends to check if the timestamp generated in the SFFormatter is in UTC timezone
   *
   * <p>It would extract the timestamp from a log record using the SFFormatter and compare it
   * against now;
   *
   * <p>Since the time difference between the log record's generated time and the current time is
   * small, their difference would be limited within TIME_DIFFERENCE_BOUNDARY if the log record's
   * timestamp is in UTC timezone
   *
   * @throws ParseException Will be thrown if date extraction fails
   */
  @Test
  public void testUTCTimeStampSimple() throws ParseException {
    TimeZone originalTz = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"));
    try {
      String record = recordGenerator.generateLogRecordString(Level.INFO, "TestMessage");

      Date date = extractDate(record);
      long nowInMs = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis();
      assertTrue(
          nowInMs - date.getTime() < TIME_DIFFERENCE_BOUNDARY,
          "Time difference boundary should be less than " + TIME_DIFFERENCE_BOUNDARY + "ms");
    } finally {
      TimeZone.setDefault(originalTz);
    }
  }

  /**
   * The bulk version test of testUTCTimeStampSimple()
   *
   * @throws ParseException Will be thrown if timestamp parsing fails
   */
  @Test
  public void testUTCTimeStampStress() throws ParseException {
    for (int i = 0; i < STRESS_TEST_ITERATION; i++) {
      testUTCTimeStampSimple();
    }
  }

  /**
   * A log generator could generate log record directly and fill in necessary field specified by
   * constructor
   */
  private class LRGenerator {
    // Required by SFFormatter as a log record without these fields would cause NullPointerException
    // in
    // our SF formatter
    // add more fields to plug in the log record if required for testing
    private String srcClassName;
    private String srcMethodName;

    private Formatter formatter;

    public LRGenerator(String srcClassName, String srcMethodName) {
      this.srcClassName = srcClassName;
      this.srcMethodName = srcMethodName;
      formatter = null;
    }

    /**
     * No formatter is needed
     *
     * @param level level of log record priority
     * @param message message of log record
     * @return A LogRecord instance
     */
    public LogRecord generateLogRecord(Level level, String message) {
      LogRecord record = new LogRecord(Level.INFO, "null");
      record.setSourceClassName(this.srcClassName);
      record.setSourceMethodName(this.srcMethodName);
      return record;
    }

    /**
     * Generate the string representation of the log record formatter is required to be not null!!
     *
     * @param level level of log record priority
     * @param message message of log record
     * @return A LogRecord instance
     */
    public String generateLogRecordString(Level level, String message) {
      return formatter.format(this.generateLogRecord(level, message));
    }

    /**
     * Formatter setter
     *
     * @param formatter The formatter to use for the log record generator
     */
    public void setFormatter(Formatter formatter) {
      this.formatter = formatter;
    }
  }

  /**
   * Helper function to extract the date from the log record
   *
   * @param string log record representation
   * @return a date specified by the log record
   * @throws ParseException Will be thrown if parsing fails
   */
  private Date extractDate(String string) throws ParseException {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    df.setCalendar(cal);
    Date date = df.parse(string);
    return date;
  }
}
