/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import net.snowflake.common.core.SqlState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.snowflake.client.jdbc.ErrorCode.INVALID_APP_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class SessionUtilTest
{

  /**
   * Test isPrefixEqual
   */
  @Test
  public void testIsPrefixEqual() throws Exception
  {
    assertThat("no port number",
               SessionUtil.isPrefixEqual(
                   "https://testaccount.snowflakecomputing.com/blah",
                   "https://testaccount.snowflakecomputing.com/"));
    assertThat("no port number with a slash",
               SessionUtil.isPrefixEqual(
                   "https://testaccount.snowflakecomputing.com/blah",
                   "https://testaccount.snowflakecomputing.com"));
    assertThat("including a port number on one of them",
               SessionUtil.isPrefixEqual(
                   "https://testaccount.snowflakecomputing.com/blah",
                   "https://testaccount.snowflakecomputing.com:443/"));

    // negative
    assertThat("different hostnames",
               !SessionUtil.isPrefixEqual(
                   "https://testaccount1.snowflakecomputing.com/blah",
                   "https://testaccount2.snowflakecomputing.com/"));
    assertThat("different port numbers",
               !SessionUtil.isPrefixEqual(
                   "https://testaccount.snowflakecomputing.com:123/blah",
                   "https://testaccount.snowflakecomputing.com:443/"));
    assertThat("different protocols",
               !SessionUtil.isPrefixEqual(
                   "http://testaccount.snowflakecomputing.com/blah",
                   "https://testaccount.snowflakecomputing.com/"));
  }

  /**
   * Test for valid values of application name in CLIENT_ENVIRONMENT field
   */
  @RunWith(Parameterized.class)
  public static class ValidAppNameTest
  {

    @Parameterized.Parameter(0)
    public String appNameInput;

    @Parameterized.Parameter(1)
    public boolean matcherMatches;

    @Parameterized.Parameter(2)
    public boolean characterCount;

    @Parameterized.Parameters
    public static Object[][] data()
    {
      return new Object[][]{
          {null, true, true},
          {"", true, true},
          {"validName.jar", true, true}
      };
    }

    // class to be tested
    private class ToTestClass
    {
      public boolean testRegex(String input) throws SQLException
      {
        String appName = SessionUtil.getApplication(input);
        Pattern p = Pattern.compile("^[\\w.-]+$");
        Matcher matcher = p.matcher(appName);
        return (matcher.matches());
      }

      public boolean testCharCount(String input) throws SQLException
      {
        String appName = SessionUtil.getApplication(input);
        return (appName.length() <= 50);
      }
    }

    @Test
    public void testValidAppName() throws SQLException
    {
      ToTestClass tester = new ToTestClass();
      assertEquals("Special Characters check", matcherMatches, tester.testRegex(appNameInput));
      assertEquals("Special Characters check", characterCount, tester.testCharCount(appNameInput));
    }
  }

  /**
   * Test for accurate SQL exceptions for invalid values of application name in CLIENT_ENVIRONMENT field
   */
  @RunWith(Parameterized.class)
  public static class InvalidAppNameTest
  {

    @Parameterized.Parameter(0)
    public String appNameInput;

    @Parameterized.Parameters
    public static Object[][] data()
    {
      return new Object[][]{
          {"extremelylongtestapplicationnamethatisfarmorethanfiftycharactersforsure"},
          {"illegal/forward/slash.jar"},
          {" no spaces allowed"},
          {"lots@of$special^chars"}
      };
    }

    // class to be tested
    private class ToTestClass
    {
      public boolean testExceptionSqlState(String input)
      {
        boolean result = false;
        try
        {
          SessionUtil.getApplication(input);
        }
        catch (SQLException e)
        {
          result = (SqlState.INVALID_PARAMETER_VALUE == e.getSQLState());
        }
        return result;
      }

      public boolean testExceptionErrorCode(String input)
      {
        boolean result = false;
        try
        {
          SessionUtil.getApplication(input);
        }
        catch (SQLException e)
        {
          result = (INVALID_APP_NAME.getMessageCode() == e.getErrorCode());
        }
        return result;
      }
    }

    @Test
    public void testInvalidAppName()
    {
      ToTestClass tester = new ToTestClass();
      assertEquals("Exception sql state check", true, tester.testExceptionSqlState(appNameInput));
      assertEquals("Exception error code check", true, tester.testExceptionErrorCode(appNameInput));
    }
  }
}
