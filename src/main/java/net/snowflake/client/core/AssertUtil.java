/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import net.snowflake.client.jdbc.ErrorCode;

/**
 * Created by jhuang on 1/27/16.
 */
public class AssertUtil
{
  /**
   * Assert the condition is true, otherwise throw an internal error
   * exception with the given message.
   *
   * @param condition
   * @param internalErrorMesg
   * @throws SFException
   */
  static void assertTrue(boolean condition, String internalErrorMesg)
  throws SFException
  {
    if (!condition)
    {
      throw new SFException(ErrorCode.INTERNAL_ERROR, internalErrorMesg);
    }
  }

}
