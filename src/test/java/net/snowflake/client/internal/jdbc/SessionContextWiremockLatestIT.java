package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import net.snowflake.client.api.connection.SnowflakeConnection;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.OTHERS)
public class SessionContextWiremockLatestIT extends BaseWiremockTest {

  private static final String SESSION_CONTEXT_MAPPING =
      "/wiremock/mappings/connection/session_context_switches.json";

  @Test
  public void testSessionContextReflectsSwitches() throws Exception {
    importMappingFromResources(SESSION_CONTEXT_MAPPING);

    Properties props = getWiremockProps();
    String connectStr = String.format("jdbc:snowflake://%s:%s", WIREMOCK_HOST, wiremockHttpPort);

    try (Connection con = DriverManager.getConnection(connectStr, props);
        Statement statement = con.createStatement()) {
      SnowflakeConnection sfCon = con.unwrap(SnowflakeConnection.class);

      assertEquals("TEST_DB", sfCon.getDatabase());
      assertEquals("TEST_SCHEMA", con.getSchema());
      assertEquals("ANALYST", sfCon.getRole());
      assertEquals("TEST_WH", sfCon.getWarehouse());

      con.setCatalog("SECOND_DB");
      assertEquals("SECOND_DB", sfCon.getDatabase());
      assertEquals("SECOND_DB", con.getCatalog());

      con.setSchema("SECOND_SCHEMA");
      assertEquals("SECOND_SCHEMA", con.getSchema());

      statement.execute("use role PUBLIC");
      assertEquals("PUBLIC", sfCon.getRole());

      statement.execute("use role ANALYST");
      assertEquals("ANALYST", sfCon.getRole());

      statement.execute("use database TEST_DB");
      assertEquals("TEST_DB", sfCon.getDatabase());

      statement.execute("use schema TEST_SCHEMA");
      assertEquals("TEST_SCHEMA", con.getSchema());
    }
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
