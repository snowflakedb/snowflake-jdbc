/*
 * Copyright (c) 2022 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import org.junit.Test;
import org.mockito.Mockito;

public class SnowflakeChunkDownloaderLatestIT extends BaseJDBCTest {

  /**
   * Tests that the chunk downloader uses the maxHttpRetries and doesn't enter and infinite loop of
   * retries.
   *
   * @throws SQLException
   * @throws InterruptedException
   */
  @Test
  public void testChunkDownloaderRetry() throws SQLException, InterruptedException {
    // set proxy to invalid host and bypass the snowflakecomputing.com domain
    // this will cause connection issues to the internal stage on fetching
    System.setProperty("https.proxyHost", "127.0.0.1");
    System.setProperty("https.proxyPort", "8080");
    System.setProperty("http.nonProxyHosts", "*snowflakecomputing.com");

    // set max retries
    Properties properties = new Properties();
    properties.put("maxHttpRetries", 2);

    SnowflakeChunkDownloader snowflakeChunkDownloaderSpy = null;

    try (Connection connection = getConnection(properties)) {
      Statement statement = connection.createStatement();
      // execute a query that will require chunk downloading
      ResultSet resultSet =
          statement.executeQuery(
              "select seq8(), randstr(1000, random()) from table(generator(rowcount => 10000))");
      List<SnowflakeResultSetSerializable> resultSetSerializables =
          ((SnowflakeResultSet) resultSet).getResultSetSerializables(100 * 1024 * 1024);
      SnowflakeResultSetSerializable resultSetSerializable = resultSetSerializables.get(0);
      SnowflakeChunkDownloader downloader =
          new SnowflakeChunkDownloader((SnowflakeResultSetSerializableV1) resultSetSerializable);
      snowflakeChunkDownloaderSpy = Mockito.spy(downloader);
      snowflakeChunkDownloaderSpy.getNextChunkToConsume();
    } catch (SnowflakeSQLException exception) {
      // verify that request was retried twice before reaching max retries
      Mockito.verify(snowflakeChunkDownloaderSpy, Mockito.times(2)).getResultStreamProvider();
      assertTrue(exception.getMessage().contains("Max retry reached for the download of #chunk0"));
      assertTrue(exception.getMessage().contains("retry=2"));
    }
  }
}
