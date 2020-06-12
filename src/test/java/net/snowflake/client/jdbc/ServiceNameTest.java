/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFSessionProperty;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Unit test for SERVICE_NAME parameter.
 */
@Ignore("powermock incompat with JDK>=9, see https://github.com/powermock/powermock/issues/901")
@RunWith(PowerMockRunner.class)
public class ServiceNameTest
{
  final static String SERVICE_NAME_KEY = "SERVICE_NAME";
  final static String INITIAL_SERVICE_NAME = "initialServiceName";
  final static String NEW_SERVICE_NAME = "newServiceName";

  @Test
  @PrepareForTest({
                      net.snowflake.client.core.HttpUtil.class
                  })

  private String responseLogin()
  {
    return "{\n" +
           "  \"data\" : {\n" +
           "  \"masterToken\" : \"masterToken\",\n" +
           "  \"token\" : \"sessionToken\",\n" +
           "  \"parameters\" : [ {\n" +
           "    \"name\" : \"" + SERVICE_NAME_KEY + "\",\n" +
           "    \"value\" : \"" + INITIAL_SERVICE_NAME + "\"\n" +
           "  } ],\n" +
           "  \"sessionInfo\" : {\n" +
           "    \"databaseName\" : \"TESTDB\",\n" +
           "    \"schemaName\" : \"TESTSCHEMA\",\n" +
           "    \"warehouseName\" : \"TESTWH\",\n" +
           "    \"roleName\" : \"TESTROLE\"\n" +
           "  },\n" +
           "  \"responseData\" : null\n" +
           "  },\n" +
           "  \"message\" : null,\n" +
           "  \"code\" : null,\n" +
           "  \"success\" : true\n" +
           "}\n";
  }

  private String responseQuery()
  {
    return "{\n" +
           "  \"data\" : {\n" +
           "    \"parameters\" : [{\"name\":\"" + SERVICE_NAME_KEY +
           "\",\"value\":\"" + NEW_SERVICE_NAME + "\"}],\n" +
           "    \"rowtype\" : [{\"name\":\"COUNT(*)\",\"database\":\"\"," +
           "\"schema\":\"\",\"table\":\"\",\"byteLength\":null,\"length\":null," +
           "\"type\":\"fixed\",\"scale\":0,\"nullable\":false,\"precision\":18} ],\n" +
           "    \"rowset\" : [[\"123456\"] ],\n" +
           "    \"total\" : 1,\n" +
           "    \"returned\" : 1,\n" +
           "    \"queryId\" : \"12345-12345-12345\",\n" +
           "    \"databaseProvider\" : null,\n" +
           "    \"finalDatabaseName\" : \"TESTDB\",\n" +
           "    \"finalSchemaName\" : \"TESTSCHEMA\",\n" +
           "    \"finalWarehouseName\" : \"TESTWH\",\n" +
           "    \"finalRoleName\" : \"TESTROLE\",\n" +
           "    \"numberOfBinds\" : 0,\n" +
           "    \"arrayBindSupported\" : false,\n" +
           "    \"statementTypeId\" : 4096,\n" +
           "    \"version\" : 1,\n" +
           "    \"sendResultTime\" : 1538693700000\n" +
           "  },\n" +
           "  \"message\" : null,\n" +
           "  \"code\" : null,\n" +
           "  \"success\" : true\n" +
           "}\n";
  }

  @Test
  @PrepareForTest({
                      net.snowflake.client.core.HttpUtil.class
                  })
  public void testAddServiceNameToRequestHeader() throws Throwable
  {
    mockStatic(HttpUtil.class);

    // login response
    when(HttpUtil.executeGeneralRequest(
        Mockito.any(HttpRequestBase.class),
        Mockito.anyInt(),
        Mockito.any(OCSPMode.class))).thenReturn(
        responseLogin());

    // query response
    when(HttpUtil.executeGeneralRequest(
        Mockito.any(HttpRequestBase.class),
        Mockito.anyInt(),
        Mockito.any(OCSPMode.class))).thenReturn(
        responseQuery());

    Properties props = new Properties();
    props.setProperty(SFSessionProperty.ACCOUNT.getPropertyKey(), "fakeaccount");
    props.setProperty(SFSessionProperty.USER.getPropertyKey(), "fakeaccount");
    props.setProperty(SFSessionProperty.PASSWORD.getPropertyKey(), "fakeaccount");
    props.setProperty(SFSessionProperty.INSECURE_MODE.getPropertyKey(), Boolean.TRUE.toString());
    SnowflakeConnectionV1 con =
        new SnowflakeConnectionV1("http://fakeaccount.snowflakecomputing.com", props);
    assertThat(con.getSfSession().getServiceName(), is(INITIAL_SERVICE_NAME));

    SnowflakeStatementV1 stmt = (SnowflakeStatementV1) con.createStatement();
    stmt.execute("SELECT 1");
    assertThat(stmt.getConnection().unwrap(SnowflakeConnectionV1.class).getSfSession()
                   .getServiceName(), is(NEW_SERVICE_NAME));
  }
}
