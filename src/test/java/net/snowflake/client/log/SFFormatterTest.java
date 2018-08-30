/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.log;

import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import static org.junit.Assert.assertTrue;

public class SFFormatterTest
{
  // Change these numbers if necessary
  /**
   * The maximum time difference in millisecond allowed for timestamp and now
   */
  private static final long TIME_DIFFERENCE_BOUNDARY = 100;
  /**
   * Number of iterations for the stress test
   */
  private static final int STRESS_TEST_ITERATION = 2000;

  /**
   * Log record generator
   */
  private LRGenerator recordGenerator;

  @Before
  public void setUp()
  {
    recordGenerator = new LRGenerator(SFFormatter.CLASS_NAME_PREFIX + "TestClass", "TestMethod");
    recordGenerator.setFormatter(new SFFormatter());
  }

  /**
   * This test intends to check if the timestamp generated in the SFFormatter is in UTC timezone
   *
   * It would extract the timestamp from a log record using the SFFormatter and compare it against now;
   *
   * Since the time difference between the log record's generated time and the current time is small,
   * their difference would be limited within TIME_DIFFERENCE_BOUNDARY if the log record's timestamp
   * is in UTC timezone
   * @throws ParseException
   */
  @Test
  public void testUTCTimeStampSimple() throws ParseException
  {
    String record = recordGenerator.generateLogRecordString(Level.INFO, "TestMessage");

    Date date = extractDate(record);
    long nowInMs = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis();
    assertTrue("Time difference boundary should be less than " + TIME_DIFFERENCE_BOUNDARY + "ms",
         nowInMs - date.getTime() < TIME_DIFFERENCE_BOUNDARY);
  }

  /**
   * The bulk version test of testUTCTimeStampSimple()
   * @throws ParseException
   */
  @Test
  public void testUTCTimeStampStress() throws ParseException
  {
    for (int i = 0; i < STRESS_TEST_ITERATION; i++) {
      testUTCTimeStampSimple();
    }
  }

  /**
   * A log generator could generate log record directly and fill in necessary field specified by constructor
   */
  private class LRGenerator
  {
    // Required by SFFormatter as a log record without these fiels would cause NullPointerException in
    // our SF formatter
    // add more fields to plug in the log record if required for testing
    private String srcClassName;
    private String srcMethodName;

    private Formatter formatter;

    public LRGenerator(String srcClassName, String srcMethodName)
    {
      this.srcClassName = srcClassName;
      this.srcMethodName = srcMethodName;
      formatter = null;
    }

    /**
     * No formatter is needed
     * @param level level of log record priority
     * @param message message of log record
     * @return A LogRecord instance
     */
    public LogRecord generateLogRecord(Level level, String message)
    {
      LogRecord record = new LogRecord(Level.INFO, "null");
      record.setSourceClassName(this.srcClassName);
      record.setSourceMethodName(this.srcMethodName);
      return record;
    }

    /**
     * Generate the string representation of the log record
     * formatter is required to be not null!!
     * @param level level of log record priority
     * @param message message of log record
     * @return A LogRecord instance
     */
    public String generateLogRecordString(Level level, String message)
    {
      return formatter.format(this.generateLogRecord(level, message));
    }

    /**
     * Formatter setter
     * @param formatter
     */
    public void setFormatter(Formatter formatter)
    {
      this.formatter = formatter;
    }
  }

  /**
   * Helper function to extract the date from the log record
   * @param string log record representation
   * @return a date specified by the log record
   * @throws ParseException
   */
  private Date extractDate(String string) throws ParseException
  {
    Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(string);
    return date;
  }

}
