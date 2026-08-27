package net.snowflake.client.internal.api.implementation.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

public class SnowflakeDatabaseMetaDataImplTablePrivilegesQueryTest {

  @Test
  public void escapeSqlStringLiteralDoublesSingleQuotes() {
    assertEquals("O''Brien", SnowflakeDatabaseMetaDataImpl.escapeSqlStringLiteral("O'Brien"));
    assertEquals("a''''b", SnowflakeDatabaseMetaDataImpl.escapeSqlStringLiteral("a''b"));
  }

  @Test
  public void tableNameQuoteDoesNotBreakOutOfTheLiteral() {
    String sql =
        SnowflakeDatabaseMetaDataImpl.buildTablePrivilegesQuery(
            "CAT", "SCH", "foo' OR '1'='1", false);
    assertEquals(
        "select * from \"CAT\".information_schema.table_privileges"
            + " where table_name = 'foo'' OR ''1''=''1'"
            + " and table_schema = 'SCH'"
            + " order by table_catalog, table_schema, table_name, privilege_type",
        sql);
    assertFalse(sql.contains("where table_name = 'foo' OR"));
  }

  @Test
  public void schemaQuoteDoesNotBreakOutOfTheLiteral() {
    String sql =
        SnowflakeDatabaseMetaDataImpl.buildTablePrivilegesQuery(
            "CAT", "SCH' OR '1'='1", "T", false);
    assertEquals(
        "select * from \"CAT\".information_schema.table_privileges"
            + " where table_name = 'T'"
            + " and table_schema = 'SCH'' OR ''1''=''1'"
            + " order by table_catalog, table_schema, table_name, privilege_type",
        sql);
  }

  @Test
  public void multistatementInjectionStaysInsideTheTableLiteral() {
    String sql =
        SnowflakeDatabaseMetaDataImpl.buildTablePrivilegesQuery(
            "CAT", null, "x'; select 11 as bar; --", false);
    assertEquals(
        "select * from \"CAT\".information_schema.table_privileges"
            + " where table_name = 'x''; select 11 as bar; --'"
            + " order by table_catalog, table_schema, table_name, privilege_type",
        sql);
  }

  @Test
  public void wildcardTableOmitsTablePredicate() {
    String sql = SnowflakeDatabaseMetaDataImpl.buildTablePrivilegesQuery("CAT", "SCH", "%", false);
    assertEquals(
        "select * from \"CAT\".information_schema.table_privileges"
            + " where table_schema = 'SCH'"
            + " order by table_catalog, table_schema, table_name, privilege_type",
        sql);
  }

  @Test
  public void catalogDoubleQuotesAreEscapedInTheIdentifier() {
    String sql =
        SnowflakeDatabaseMetaDataImpl.buildTablePrivilegesQuery("dbwith\"quotes", null, "T", false);
    assertEquals(
        "select * from \"dbwith\"\"quotes\".information_schema.table_privileges"
            + " where table_name = 'T'"
            + " order by table_catalog, table_schema, table_name, privilege_type",
        sql);
  }
}
