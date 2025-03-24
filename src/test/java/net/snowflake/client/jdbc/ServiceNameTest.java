package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFSessionProperty;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Unit test for SERVICE_NAME parameter. */
public class ServiceNameTest {
  static final String SERVICE_NAME_KEY = "SERVICE_NAME";
  static final String INITIAL_SERVICE_NAME = "initialServiceName";
  static final String NEW_SERVICE_NAME = "newServiceName";
  static final String AUTOCOMMIT_KEY = "AUTOCOMMIT";
  static final String AUTOCOMMIT_VAL = "true";

  private String responseLogin() {
    return "{\n"
        + "  \"data\" : {\n"
        + "  \"masterToken\" : \"masterToken\",\n"
        + "  \"token\" : \"sessionToken\",\n"
        + "  \"parameters\" : [ {\n"
        + "    \"name\" : \""
        + SERVICE_NAME_KEY
        + "\",\n"
        + "    \"value\" : \""
        + INITIAL_SERVICE_NAME
        + "\"\n"
        + "  }, {\n"
        + "    \"name\" : \""
        + AUTOCOMMIT_KEY
        + "\",\n"
        + "    \"value\" : \""
        + AUTOCOMMIT_VAL
        + "\"\n"
        + " }],\n"
        + "  \"sessionInfo\" : {\n"
        + "    \"databaseName\" : \"TESTDB\",\n"
        + "    \"schemaName\" : \"TESTSCHEMA\",\n"
        + "    \"warehouseName\" : \"TESTWH\",\n"
        + "    \"roleName\" : \"TESTROLE\"\n"
        + "  },\n"
        + "  \"responseData\" : null\n"
        + "  },\n"
        + "  \"message\" : null,\n"
        + "  \"code\" : null,\n"
        + "  \"success\" : true\n"
        + "}\n";
  }

  private String responseQuery() {
    return "{\n"
        + "  \"data\" : {\n"
        + "    \"parameters\" : [{\"name\":\""
        + SERVICE_NAME_KEY
        + "\",\"value\":\""
        + NEW_SERVICE_NAME
        + "\"}],\n"
        + "    \"rowtype\" : [{\"name\":\"COUNT(*)\",\"database\":\"\","
        + "\"schema\":\"\",\"table\":\"\",\"byteLength\":null,\"length\":null,"
        + "\"type\":\"fixed\",\"scale\":0,\"nullable\":false,\"precision\":18} ],\n"
        + "    \"rowset\" : [[\"123456\"] ],\n"
        + "    \"total\" : 1,\n"
        + "    \"returned\" : 1,\n"
        + "    \"queryId\" : \"12345-12345-12345\",\n"
        + "    \"databaseProvider\" : null,\n"
        + "    \"finalDatabaseName\" : \"TESTDB\",\n"
        + "    \"finalSchemaName\" : \"TESTSCHEMA\",\n"
        + "    \"finalWarehouseName\" : \"TESTWH\",\n"
        + "    \"finalRoleName\" : \"TESTROLE\",\n"
        + "    \"numberOfBinds\" : 0,\n"
        + "    \"arrayBindSupported\" : false,\n"
        + "    \"statementTypeId\" : 4096,\n"
        + "    \"version\" : 1,\n"
        + "    \"sendResultTime\" : 1538693700000\n"
        + "  },\n"
        + "  \"message\" : null,\n"
        + "  \"code\" : null,\n"
        + "  \"success\" : true\n"
        + "}\n";
  }

  @Test
  public void testAddServiceNameToRequestHeader() throws Throwable {
    try (MockedStatic<HttpUtil> mockedHttpUtil = Mockito.mockStatic(HttpUtil.class)) {
      mockedHttpUtil
          .when(
              () ->
                  HttpUtil.executeGeneralRequest(
                      Mockito.any(HttpRequestBase.class),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.any(HttpClientSettingsKey.class)))
          .thenReturn(responseLogin());
      mockedHttpUtil
          .when(
              () ->
                  HttpUtil.executeRequest(
                      Mockito.any(HttpRequestBase.class),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.any(AtomicBoolean.class),
                      Mockito.anyBoolean(),
                      Mockito.anyBoolean(),
                      Mockito.any(HttpClientSettingsKey.class),
                      Mockito.any(ExecTimeTelemetryData.class)))
          .thenReturn(responseQuery());

      Properties props = new Properties();
      props.setProperty(SFSessionProperty.ACCOUNT.getPropertyKey(), "fakeaccount");
      props.setProperty(SFSessionProperty.USER.getPropertyKey(), "fakeuser");
      props.setProperty(SFSessionProperty.PASSWORD.getPropertyKey(), "fakepassword");
      props.setProperty(SFSessionProperty.INSECURE_MODE.getPropertyKey(), Boolean.TRUE.toString());
      try (SnowflakeConnectionV1 con =
          new SnowflakeConnectionV1(
              "jdbc:snowflake://http://fakeaccount.snowflakecomputing.com", props)) {
        assertThat(con.getSfSession().getServiceName(), is(INITIAL_SERVICE_NAME));

        try (SnowflakeStatementV1 stmt = (SnowflakeStatementV1) con.createStatement()) {
          stmt.execute("SELECT 1");
          assertThat(
              stmt.getConnection()
                  .unwrap(SnowflakeConnectionV1.class)
                  .getSfSession()
                  .getServiceName(),
              is(NEW_SERVICE_NAME));
        }
      }
    }
  }
}
