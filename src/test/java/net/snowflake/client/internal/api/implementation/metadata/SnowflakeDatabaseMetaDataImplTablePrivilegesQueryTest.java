package net.snowflake.client.internal.api.implementation.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class SnowflakeDatabaseMetaDataImplTablePrivilegesQueryTest {

  @Test
  public void escapeSqlStringLiteralDoublesSingleQuotes() {
    assertNull(SnowflakeDatabaseMetaDataImpl.escapeSqlStringLiteral(null));
    assertEquals("ORDERS", SnowflakeDatabaseMetaDataImpl.escapeSqlStringLiteral("ORDERS"));
    assertEquals("O''Brien", SnowflakeDatabaseMetaDataImpl.escapeSqlStringLiteral("O'Brien"));
    assertEquals("a''''b", SnowflakeDatabaseMetaDataImpl.escapeSqlStringLiteral("a''b"));
    assertEquals(
        "foo'' OR ''1''=''1",
        SnowflakeDatabaseMetaDataImpl.escapeSqlStringLiteral("foo' OR '1'='1"));
    assertEquals(
        "x''; select 11 as bar; --",
        SnowflakeDatabaseMetaDataImpl.escapeSqlStringLiteral("x'; select 11 as bar; --"));
  }
}
