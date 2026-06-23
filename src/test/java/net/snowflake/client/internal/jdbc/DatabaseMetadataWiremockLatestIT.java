package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Deterministic coverage for the {@link DatabaseMetaData} SHOW-command result mapping. These tests
 * stub the GS responses with WireMock so the JDBC mapping logic (building the SHOW command and
 * mapping the result columns into the JDBC metadata ResultSet shape) is validated without depending
 * on a live Snowflake account or on eventually-consistent server metadata. The corresponding live
 * integration tests (e.g. {@code SnowflakeDriverIT.testDBMetadata}, {@code
 * DatabaseMetaDataIT.testGetPrimarykeys}, {@code SnowflakeDriverIT.testConstraints}) remain as
 * integration smoke coverage but are inherently timing-sensitive; these tests guard the mapping
 * logic deterministically.
 */
@Tag(TestTags.OTHERS)
public class DatabaseMetadataWiremockLatestIT extends BaseWiremockTest {

  private static final String METADATA_MAPPING =
      "/wiremock/mappings/databasemetadata/metadata_show_results.json";

  @Test
  public void testGetTablesMapsShowObjectsResult() throws Exception {
    importMappingFromResources(METADATA_MAPPING);

    try (Connection con = getWiremockConnection()) {
      DatabaseMetaData metaData = con.getMetaData();
      try (ResultSet rs = metaData.getTables("TEST_DB", "TEST_SCHEMA", "ORDERS", null)) {
        assertTrue(rs.next(), "expected the stubbed table to be returned");
        assertEquals("TEST_DB", rs.getString("TABLE_CAT"));
        assertEquals("TEST_SCHEMA", rs.getString("TABLE_SCHEM"));
        assertEquals("ORDERS", rs.getString("TABLE_NAME"));
        assertEquals("TABLE", rs.getString("TABLE_TYPE"));
        assertEquals("a test orders table", rs.getString("REMARKS"));
        assertFalse(rs.next(), "expected exactly one table row");
      }
    }
  }

  @Test
  public void testGetPrimaryKeysMapsShowPrimaryKeysResult() throws Exception {
    importMappingFromResources(METADATA_MAPPING);

    try (Connection con = getWiremockConnection()) {
      DatabaseMetaData metaData = con.getMetaData();
      try (ResultSet rs = metaData.getPrimaryKeys("TEST_DB", "TEST_SCHEMA", "PK_TEST")) {
        assertTrue(rs.next(), "expected the stubbed primary key to be returned");
        assertEquals("TEST_DB", rs.getString("TABLE_CAT"));
        assertEquals("TEST_SCHEMA", rs.getString("TABLE_SCHEM"));
        assertEquals("PK_TEST", rs.getString("TABLE_NAME"));
        assertEquals("C1", rs.getString("COLUMN_NAME"));
        assertEquals(1, rs.getInt("KEY_SEQ"));
        assertNotNull(rs.getString("PK_NAME"));
        assertFalse(rs.next(), "expected exactly one primary key row");
      }
    }
  }

  @Test
  public void testGetImportedKeysMapsShowImportedKeysResult() throws Exception {
    importMappingFromResources(METADATA_MAPPING);

    try (Connection con = getWiremockConnection()) {
      DatabaseMetaData metaData = con.getMetaData();
      try (ResultSet rs = metaData.getImportedKeys("TEST_DB", "TEST_SCHEMA", "FK_TEST")) {
        assertTrue(rs.next(), "expected the stubbed imported key to be returned");
        assertEquals("PK_TEST", rs.getString("PKTABLE_NAME"));
        assertEquals("C1", rs.getString("PKCOLUMN_NAME"));
        assertEquals("FK_TEST", rs.getString("FKTABLE_NAME"));
        assertEquals("C1", rs.getString("FKCOLUMN_NAME"));
        assertEquals(1, rs.getInt("KEY_SEQ"));
        assertFalse(rs.next(), "expected exactly one imported key row");
      }
    }
  }

  private Connection getWiremockConnection() throws Exception {
    String connectStr = String.format("jdbc:snowflake://%s:%s", WIREMOCK_HOST, wiremockHttpPort);
    return DriverManager.getConnection(connectStr, getWiremockProps());
  }

  private static Properties getWiremockProps() {
    Properties props = new Properties();
    props.put("account", "testaccount");
    props.put("user", "testuser");
    props.put("password", "testpassword");
    props.put("warehouse", "testwh");
    props.put("database", "TEST_DB");
    props.put("schema", "TEST_SCHEMA");
    props.put("ssl", "off");
    props.put("insecureMode", "true");
    return props;
  }
}
