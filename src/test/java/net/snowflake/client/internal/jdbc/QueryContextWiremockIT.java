package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl;
import net.snowflake.client.internal.core.QueryContextDTO;
import net.snowflake.client.internal.core.QueryContextEntryDTO;
import net.snowflake.client.internal.core.SFSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.OTHERS)
public class QueryContextWiremockIT extends BaseWiremockTest {

  private static final String QCC_FAILED_QUERY_MAPPING =
      "/wiremock/mappings/querycontext/qcc-merge-on-failed-query.json";

  /**
   * SNOW-3063492: Verify that QueryContext is merged from the server response even when the query
   * fails. The WireMock stub returns a duplicate primary key error with queryContext entries.
   * Before the fix, the driver would throw the exception without merging QCC.
   */
  @Test
  public void testQueryContextMergedOnFailedQuery() throws Exception {
    importMappingFromResources(QCC_FAILED_QUERY_MAPPING);

    Properties props = getWiremockProps();
    String connectStr = String.format("jdbc:snowflake://%s:%s", WIREMOCK_HOST, wiremockHttpPort);

    Connection conn = DriverManager.getConnection(connectStr, props);
    SFSession session = conn.unwrap(SnowflakeConnectionImpl.class).getSfSession();
    Statement stmt = conn.createStatement();

    assertThrows(
        SQLException.class,
        () -> stmt.executeQuery("INSERT INTO hybrid_test VALUES ('key1', 'duplicate')"));

    QueryContextDTO qcc = session.getQueryContextDTO();
    assertNotNull(qcc, "QueryContext should have been merged from the failed response");

    List<QueryContextEntryDTO> entries = qcc.getEntries();
    assertNotNull(entries);
    assertEquals(2, entries.size());

    assertEquals(0, entries.get(0).getId());
    assertEquals(1775206502642407L, entries.get(0).getTimestamp());
    assertEquals(0, entries.get(0).getPriority());

    assertEquals(69924, entries.get(1).getId());
    assertEquals(1775206502558790L, entries.get(1).getTimestamp());
    assertEquals(1, entries.get(1).getPriority());
  }

  private static Properties getWiremockProps() {
    Properties props = new Properties();
    props.put("account", "testaccount");
    props.put("user", "testuser");
    props.put("password", "testpassword");
    props.put("warehouse", "testwh");
    props.put("database", "testdb");
    props.put("schema", "testschema");
    props.put("ssl", "off");
    props.put("insecureMode", "true");
    return props;
  }
}
