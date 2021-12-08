/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.BooleanNode;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.jdbc.MockConnectionTest;
import org.junit.Test;

public class SessionUtilTest {

  /** Test isPrefixEqual */
  @Test
  public void testIsPrefixEqual() throws Exception {
    assertThat(
        "no port number",
        SessionUtil.isPrefixEqual(
            "https://testaccount.snowflakecomputing.com/blah",
            "https://testaccount.snowflakecomputing.com/"));
    assertThat(
        "no port number with a slash",
        SessionUtil.isPrefixEqual(
            "https://testaccount.snowflakecomputing.com/blah",
            "https://testaccount.snowflakecomputing.com"));
    assertThat(
        "including a port number on one of them",
        SessionUtil.isPrefixEqual(
            "https://testaccount.snowflakecomputing.com/blah",
            "https://testaccount.snowflakecomputing.com:443/"));

    // negative
    assertThat(
        "different hostnames",
        !SessionUtil.isPrefixEqual(
            "https://testaccount1.snowflakecomputing.com/blah",
            "https://testaccount2.snowflakecomputing.com/"));
    assertThat(
        "different port numbers",
        !SessionUtil.isPrefixEqual(
            "https://testaccount.snowflakecomputing.com:123/blah",
            "https://testaccount.snowflakecomputing.com:443/"));
    assertThat(
        "different protocols",
        !SessionUtil.isPrefixEqual(
            "http://testaccount.snowflakecomputing.com/blah",
            "https://testaccount.snowflakecomputing.com/"));
  }

  @Test
  public void testParameterParsing() {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("other_parameter", BooleanNode.getTrue());
    SFBaseSession session = new MockConnectionTest.MockSnowflakeConnectionImpl().getSFSession();
    SessionUtil.updateSfDriverParamValues(parameterMap, session);
    assert (((BooleanNode) session.getOtherParameter("other_parameter")).asBoolean());
  }

  @Test
  public void testConvertSystemPropertyToIntValue() {
    // Test that setting real value works
    System.setProperty("net.snowflake.jdbc.max_connections", "500");
    assertEquals(
        500,
        HttpUtil.convertSystemPropertyToIntValue(
            HttpUtil.JDBC_MAX_CONNECTIONS_PROPERTY, HttpUtil.DEFAULT_MAX_CONNECTIONS));
    // Test that entering a non-int sets the value to the default
    System.setProperty("net.snowflake.jdbc.max_connections", "notAnInteger");
    assertEquals(
        HttpUtil.DEFAULT_MAX_CONNECTIONS,
        HttpUtil.convertSystemPropertyToIntValue(
            HttpUtil.JDBC_MAX_CONNECTIONS_PROPERTY, HttpUtil.DEFAULT_MAX_CONNECTIONS));
    // Test another system property
    System.setProperty("net.snowflake.jdbc.max_connections_per_route", "30");
    assertEquals(
        30,
        HttpUtil.convertSystemPropertyToIntValue(
            HttpUtil.JDBC_MAX_CONNECTIONS_PER_ROUTE_PROPERTY,
            HttpUtil.DEFAULT_MAX_CONNECTIONS_PER_ROUTE));
  }
}
