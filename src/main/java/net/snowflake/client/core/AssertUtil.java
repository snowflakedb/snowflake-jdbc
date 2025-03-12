package net.snowflake.client.core;

import net.snowflake.client.jdbc.ErrorCode;

public class AssertUtil {
  /**
   * Assert the condition is true, otherwise throw an internal error exception with the given
   * message.
   *
   * @param condition The variable to test the 'truthiness' of
   * @param internalErrorMesg The error message to display if condition is false
   * @throws SFException Will be thrown if condition is false
   */
  @SnowflakeJdbcInternalApi
  public static void assertTrue(boolean condition, String internalErrorMesg) throws SFException {
    if (!condition) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, internalErrorMesg);
    }
  }
}
