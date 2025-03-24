package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Tag(TestTags.CORE)
public class SnowflakeChunkDownloaderLatestIT extends BaseJDBCTest {
  private static String originalProxyHost;
  private static String originalProxyPort;
  private static String originalNonProxyHosts;

  @BeforeAll
  public static void setUp() throws Exception {
    originalProxyHost = System.getProperty("https.proxyHost");
    originalProxyPort = System.getProperty("https.proxyPort");
    originalNonProxyHosts = System.getProperty("https.nonProxyHosts");
  }

  private static void restoreProperty(String key, String value) {
    if (value != null) {
      System.setProperty(key, value);
    } else {
      System.clearProperty(key);
    }
  }

  @AfterAll
  public static void tearDown() throws Exception {
    restoreProperty("https.proxyHost", originalProxyHost);
    restoreProperty("https.proxyPort", originalProxyPort);
    restoreProperty("https.nonProxyHosts", originalNonProxyHosts);
  }
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

    try (Connection connection = getConnection(properties);
        Statement statement = connection.createStatement()) {
      // execute a query that will require chunk downloading
      try (ResultSet resultSet =
          statement.executeQuery(
              "select seq8(), randstr(1000, random()) from table(generator(rowcount => 10000))")) {
        List<SnowflakeResultSetSerializable> resultSetSerializables =
            ((SnowflakeResultSet) resultSet).getResultSetSerializables(100 * 1024 * 1024);
        SnowflakeResultSetSerializable resultSetSerializable = resultSetSerializables.get(0);
        SnowflakeChunkDownloader downloader =
            new SnowflakeChunkDownloader((SnowflakeResultSetSerializableV1) resultSetSerializable);
        SnowflakeChunkDownloader snowflakeChunkDownloaderSpy = Mockito.spy(downloader);
        SnowflakeSQLException exception =
            assertThrows(
                SnowflakeSQLException.class, snowflakeChunkDownloaderSpy::getNextChunkToConsume);
        Mockito.verify(snowflakeChunkDownloaderSpy, Mockito.times(2)).getResultStreamProvider();
        assertTrue(
            exception.getMessage().contains("Max retry reached for the download of chunk#0"));
        assertTrue(exception.getMessage().contains("retry: 2"));
      }
    }
  }
}
