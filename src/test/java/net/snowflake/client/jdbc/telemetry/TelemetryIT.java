/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestCategoryCore;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(TestCategoryCore.class)
public class TelemetryIT extends AbstractDriverIT
{
  private Connection connection = null;
  private static ObjectMapper mapper = new ObjectMapper();

  @Before
  public void init() throws SQLException
  {
    this.connection = getConnection();
  }

  @Test
  public void test() throws Exception
  {
    TelemetryClient telemetry =
        (TelemetryClient) TelemetryClient.createTelemetry(connection, 100);
    ObjectNode node1 = mapper.createObjectNode();
    node1.put("type", "query");
    node1.put("query_id", "sdasdasdasdasds");
    ObjectNode node2 = mapper.createObjectNode();
    node2.put("type", "query");
    node2.put("query_id", "eqweqweqweqwe");
    telemetry.addLogToBatch(node1, 1234567);
    telemetry.addLogToBatch(new TelemetryData(node2, 22345678));
    assertEquals(telemetry.bufferSize(), 2);

    assertTrue(telemetry.sendBatchAsync().get());
    assertEquals(telemetry.bufferSize(), 0);

    assertTrue(telemetry.sendLog(node1, 1234567));
    assertEquals(telemetry.bufferSize(), 0);

    assertTrue(telemetry.sendLog(new TelemetryData(node2, 22345678)));
    assertEquals(telemetry.bufferSize(), 0);

    //reach flush threshold
    for (int i = 0; i < 99; i++)
    {
      telemetry.addLogToBatch(node1, 1111);
    }
    assertEquals(telemetry.bufferSize(), 99);
    telemetry.addLogToBatch(node1, 222);

    // flush is async, sleep some time and then check buffer size
    Thread.sleep(1000);
    assertEquals(telemetry.bufferSize(), 0);

    telemetry.addLogToBatch(node1, 111);
    assertEquals(telemetry.bufferSize(), 1);

    assertFalse(telemetry.isClosed());
    telemetry.close();
    assertTrue(telemetry.isClosed());
    //close function sends the metrics to the server
    assertEquals(telemetry.bufferSize(), 0);
  }

  @Test
  public void close1()
  {
    TelemetryClient telemetry =
        (TelemetryClient) TelemetryClient.createTelemetry(connection);
    telemetry.close();
    ObjectNode node = mapper.createObjectNode();
    node.put("type", "query");
    node.put("query_id", "sdasdasdasdasds");
    telemetry.addLogToBatch(node, 1234567);
  }

  @Test
  public void close2()
  {
    TelemetryClient telemetry = (TelemetryClient) TelemetryClient.createTelemetry(connection);
    telemetry.close();
    ObjectNode node = mapper.createObjectNode();
    node.put("type", "query");
    node.put("query_id", "sdasdasdasdasds");
    telemetry.addLogToBatch(new TelemetryData(node, 1234567));
  }

  @Test
  public void close3()
  {
    TelemetryClient telemetry =
        (TelemetryClient) TelemetryClient.createTelemetry(connection);
    telemetry.close();
    telemetry.close();
  }

  @Test
  public void test4() throws Exception
  {
    TelemetryClient telemetry =
        (TelemetryClient) TelemetryClient.createTelemetry(connection, 100);

    ObjectNode node1 = mapper.createObjectNode();
    node1.put("type", "query");
    node1.put("query_id", "sdasdasdasdasds");
    ObjectNode node2 = mapper.createObjectNode();
    node2.put("type", "query");
    node2.put("query_id", "eqweqweqweqwe");
    telemetry.addLogToBatch(node1, 1234567);

    assertEquals(telemetry.bufferSize(), 1);
    telemetry.disableTelemetry();
    assertFalse(telemetry.isTelemetryEnabled());

    telemetry.addLogToBatch(new TelemetryData(node2, 22345678));
    assertEquals(telemetry.bufferSize(), 1);

    assertFalse(telemetry.sendBatchAsync().get());
    assertEquals(telemetry.bufferSize(), 1);

    assertFalse(telemetry.sendLog(node1, 1234567));
    assertEquals(telemetry.bufferSize(), 1);

    assertFalse(telemetry.sendLog(new TelemetryData(node2, 22345678)));
    assertEquals(telemetry.bufferSize(), 1);

  }

}
