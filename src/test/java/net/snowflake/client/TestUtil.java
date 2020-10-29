/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client;

import net.snowflake.client.core.SFException;
import org.junit.Assert;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestUtil
{
  /**
   * Util function to assert a piece will throw exception and assert on the
   * error code
   *
   * @param errorCode expected error code
   * @param testCode  the code that will run and throws exception
   */
  public static void assertSFException(int errorCode, TestRunInterface testCode)
  {
    try
    {
      testCode.run();
      Assert.fail();
    }
    catch (SFException e)
    {
      assertThat(e.getVendorCode(), is(errorCode));
    }
  }

  /**
   * Functional interface used to run a piece of code which throws SFException
   */
  @FunctionalInterface
  public interface TestRunInterface
  {
    void run() throws SFException;
  }
}
