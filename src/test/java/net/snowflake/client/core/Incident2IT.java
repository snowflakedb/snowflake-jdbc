/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core;

import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnTravisCI;
import net.snowflake.client.category.TestCategoryCore;
import org.junit.Test;

import java.sql.SQLException;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.experimental.categories.Category;

/**
 * Incident v1 framework additional IT
 */
@Category(TestCategoryCore.class)
public class Incident2IT extends BaseIncidentTest
{
  /**
   * Tests Unthrottle client side incident.
   * <p>
   * This tests should run against a new JVM process since
   * snowflake.throttle_duration can only be reset when JVM
   * started and use command line option
   * Use following command to run this tests:
   * <p>
   * mvn -Dit.test=Incident2IT
   * -Dsnowflake.throttle_duration=0
   * -Dsnowflake.throttle_limit=10
   * failsafe:integration-test
   * And use this command to all the rest tests
   * <p>
   * mvn -Dit.test=\!Incident2IT failsafe:integration-test
   *
   * @throws SQLException if any error occurs
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testUnthrottleIncidentsInClientSide() throws SQLException
  {
    try
    {
      String signature = "testUnthrottleIncidentsInClientSide"
                         + RandomStringUtils.randomAlphabetic(5);
      for (int i = 0; i < 8; i++)
      {
        generateIncidentWithSignature(signature, true);
      }
      // gs now throttles incidents
      try
      {
        verifyIncidentRegisteredInGS(signature, 1);
      }
      catch (AssertionError err)
      {
        // test is flaky. its output seems to depend on the number of GS instances running
        verifyIncidentRegisteredInGS(signature, 2);
      }
    }
    finally
    {
      System.clearProperty("snowflake.throttle_duration");
      System.clearProperty("snowflake.throttle_limit");
    }
  }
}
