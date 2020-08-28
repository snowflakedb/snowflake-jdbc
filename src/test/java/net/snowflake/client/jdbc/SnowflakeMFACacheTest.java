/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.ObjectMapperFactory;
import org.apache.http.client.methods.HttpPost;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;

class Buddy {
  static String name(int x) {
    return "John";
  }
}

public class SnowflakeMFACacheTest extends BaseJDBCTest {
  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

  private ObjectNode getMockedHttpResponse() {
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

    respNode.set("data", dataNode);
    respNode.put("success", true);
    respNode.put("message", "msg");

    return respNode;
  }

  @Test
  public void testNormalConnection() throws SQLException, IOException {
    String ret = getMockedHttpResponse().toString();
    try (MockedStatic<HttpUtil> theMock = Mockito.mockStatic(HttpUtil.class)) {
      theMock.when(() -> HttpUtil.executeGeneralRequest(any(HttpPost.class), anyInt(), any(OCSPMode.class))).thenReturn(ret);
      Connection con = getConnection();
      assertFalse(con.isClosed());
      con.close();
      assertTrue(con.isClosed());
    }
  }
}
