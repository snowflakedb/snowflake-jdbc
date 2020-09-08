/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import net.snowflake.client.category.TestCategoryConnection;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.ObjectMapperFactory;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpPost;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

class Buddy {
  static String name() {
    return "John";
  }
}

@Category(TestCategoryConnection.class)
public class SnowflakeMFACacheTest {
  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
  private static final String[] mockedMfaToken = {"mockedMfaToken0", "mockedMfaToken1"};

  private ObjectNode getNormalMockedHttpResponse(boolean success, int mfaTokenIdx) {
    ObjectNode respNode = mapper.createObjectNode();
    ObjectNode dataNode = mapper.createObjectNode();
    ArrayNode paraArray = mapper.createArrayNode();
    ObjectNode autocommit = mapper.createObjectNode();

    autocommit.put("name", "AUTOCOMMIT");
    autocommit.put("value", true);
    paraArray.add(autocommit);

    dataNode.set("parameters", paraArray);
    dataNode.put("masterToken", "mockedMasterToken");
    dataNode.put("token", "mockedToken");
    if (mfaTokenIdx >= 0) {
      dataNode.put("mfaToken", mockedMfaToken[mfaTokenIdx]);
    }

    respNode.set("data", dataNode);
    respNode.put("success", success);
    respNode.put("message", "msg");

    return respNode;
  }

  private JsonNode parseRequest(HttpPost post) throws IOException {
    StringWriter writer = null;
    String theString;
    try {
      writer = new StringWriter();
      try (InputStream ins = post.getEntity().getContent()) {
        IOUtils.copy(ins, writer, "UTF-8");
      }
      theString = writer.toString();
    } finally {
      IOUtils.closeQuietly(writer);
    }

    JsonNode jsonNode = mapper.readTree(theString);
    return jsonNode;
  }

  private Properties getBaseProp() {
    Properties prop = new Properties();
    prop.put("account", "testaccount");
    prop.put("user", "testuser");
    prop.put("password", "testpassword");
    prop.put("authenticator", "username_password_mfa");
    prop.put("CLIENT_REQUEST_MFA_TOKEN", true);
    return prop;
  }

  // Sample code for how to use mockito static mocking feature.
  @Test
  public void testMockitoStaticMockSample() throws SQLException, IOException {
    String ret = getNormalMockedHttpResponse(true, 0).toString();
    assertTrue(Buddy.name() == "John");
    try (MockedStatic<HttpUtil> theMock = Mockito.mockStatic(HttpUtil.class);
        MockedStatic<Buddy> mockBuddy = Mockito.mockStatic(Buddy.class)) {
      theMock
          .when(
              () ->
                  HttpUtil.executeGeneralRequest(
                      any(HttpPost.class), anyInt(), any(OCSPMode.class)))
          .thenReturn(ret);
      mockBuddy.when(Buddy::name).thenReturn("Daddy");
      assertTrue(Buddy.name() == "Daddy");

      Properties prop = getBaseProp();
      String url = "jdbc:snowflake://testaccount.snowflakecomputing.com";
      Connection con = DriverManager.getConnection(url, prop);
      assertFalse(con.isClosed());
      con.close();
      assertTrue(con.isClosed());
      assertFalse(Buddy.name() == "John");
    }
  }

  @Test
  public void testMFAFunctionality() throws SQLException {
    try (MockedStatic<HttpUtil> mockedHttpUtil = Mockito.mockStatic(HttpUtil.class); ) {
      mockedHttpUtil
          .when(
              () ->
                  HttpUtil.executeGeneralRequest(
                      any(HttpPost.class), anyInt(), any(OCSPMode.class)))
          .thenAnswer(
              new Answer<String>() {
                int callCount = 0;

                @Override
                public String answer(InvocationOnMock invocation) throws Throwable {
                  String res;
                  JsonNode jsonNode;
                  final Object[] args = invocation.getArguments();

                  if (callCount == 0) {
                    jsonNode = parseRequest((HttpPost) args[0]);
                    assertTrue(
                        jsonNode
                            .path("data")
                            .path("SESSION_PARAMETERS")
                            .path("CLIENT_REQUEST_MFA_TOKEN")
                            .asBoolean());
                    res = getNormalMockedHttpResponse(true, 0).toString();
                  } else if (callCount == 1) {
                    res = getNormalMockedHttpResponse(true, -1).toString();
                  } else if (callCount == 2) {
                    jsonNode = parseRequest((HttpPost) args[0]);
                    assertTrue(
                        jsonNode
                            .path("data")
                            .path("SESSION_PARAMETERS")
                            .path("CLIENT_REQUEST_MFA_TOKEN")
                            .asBoolean());
                    assertEquals(jsonNode.path("data").path("TOKEN").asText(), mockedMfaToken[0]);
                    res = getNormalMockedHttpResponse(true, 1).toString();
                  } else if (callCount == 3) {
                    res = getNormalMockedHttpResponse(true, -1).toString();
                  } else if (callCount == 4) {
                    jsonNode = parseRequest((HttpPost) args[0]);
                    assertTrue(
                        jsonNode
                            .path("data")
                            .path("SESSION_PARAMETERS")
                            .path("CLIENT_REQUEST_MFA_TOKEN")
                            .asBoolean());
                    assertEquals(jsonNode.path("data").path("TOKEN").asText(), mockedMfaToken[1]);
                    res = getNormalMockedHttpResponse(true, -1).toString();
                  } else if (callCount == 5) {
                    res = getNormalMockedHttpResponse(true, -1).toString();
                  } else {
                    res = getNormalMockedHttpResponse(false, -1).toString();
                  }

                  callCount += 1; // this will be incremented on both connecting and closing
                  return res;
                }
              });

      Properties prop = getBaseProp();

      // connect url
      String url = "jdbc:snowflake://testaccount.snowflakecomputing.com";
      Connection con = DriverManager.getConnection(url, prop);
      con.close();
      Connection con1 = DriverManager.getConnection(url, prop);
      con1.close();
      Connection con2 = DriverManager.getConnection(url, prop);
      con2.close();
    }
  }
}
