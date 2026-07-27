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
import java.util.concurrent.TimeUnit;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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

  /**
   * With {@code enablePatternSearch=true} (the default), a wildcard schema is treated as a LIKE
   * pattern, so the client-side filter matches the stubbed {@code TEST_SCHEMA} row. This
   * deterministically covers the pattern-search behavior previously exercised by the flaky live
   * {@code testPatternSearchAllowedForPrimaryAndForeignKeys}.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  public void testPatternSearchEnabledTreatsSchemaAsWildcard() throws Exception {
    importMappingFromResources(METADATA_MAPPING);

    Properties props = getWiremockProps();
    props.put("enablePatternSearch", "true");
    try (Connection con = getWiremockConnection(props)) {
      DatabaseMetaData metaData = con.getMetaData();
      try (ResultSet rs = metaData.getPrimaryKeys("TEST_DB", "TEST%", "PK_TEST")) {
        assertEquals(1, countRows(rs), "wildcard schema should match with pattern search enabled");
      }
      try (ResultSet rs = metaData.getImportedKeys("TEST_DB", "TEST%", "FK_TEST")) {
        assertEquals(1, countRows(rs), "wildcard schema should match with pattern search enabled");
      }
    }
  }

  /**
   * With {@code enablePatternSearch=false}, the schema/table arguments are treated as exact
   * literals: an exact name still matches, but a wildcard does not. This deterministically covers
   * the behavior previously exercised by the flaky live {@code
   * testNoPatternSearchAllowedForPrimaryAndForeignKeys}.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  public void testPatternSearchDisabledTreatsSchemaAsLiteral() throws Exception {
    importMappingFromResources(METADATA_MAPPING);

    Properties props = getWiremockProps();
    props.put("enablePatternSearch", "false");
    try (Connection con = getWiremockConnection(props)) {
      DatabaseMetaData metaData = con.getMetaData();
      // exact schema still matches
      try (ResultSet rs = metaData.getPrimaryKeys("TEST_DB", "TEST_SCHEMA", "PK_TEST")) {
        assertEquals(1, countRows(rs), "exact schema should match with pattern search disabled");
      }
      // wildcard schema is treated literally and therefore does not match
      try (ResultSet rs = metaData.getPrimaryKeys("TEST_DB", "TEST%", "PK_TEST")) {
        assertEquals(
            0, countRows(rs), "wildcard schema should not match with pattern search disabled");
      }
      try (ResultSet rs = metaData.getImportedKeys("TEST_DB", "TEST%", "FK_TEST")) {
        assertEquals(
            0, countRows(rs), "wildcard schema should not match with pattern search disabled");
      }
    }
  }

  private static int countRows(ResultSet rs) throws Exception {
    int count = 0;
    while (rs.next()) {
      count++;
    }
    return count;
  }

  private Connection getWiremockConnection() throws Exception {
    return getWiremockConnection(getWiremockProps());
  }

  private Connection getWiremockConnection(Properties props) throws Exception {
    String connectStr = String.format("jdbc:snowflake://%s:%s", WIREMOCK_HOST, wiremockHttpPort);
    return DriverManager.getConnection(connectStr, props);
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
