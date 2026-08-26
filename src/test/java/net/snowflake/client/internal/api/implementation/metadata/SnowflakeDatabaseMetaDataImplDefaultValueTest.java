package net.snowflake.client.internal.api.implementation.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class SnowflakeDatabaseMetaDataImplDefaultValueTest {

  @Test
  public void nullDefaultDoesNotThrowAndStaysNull() {
    assertNull(SnowflakeDatabaseMetaDataImpl.normalizeColumnDefaultValue(null, false));
    assertNull(SnowflakeDatabaseMetaDataImpl.normalizeColumnDefaultValue(null, true));
  }

  @Test
  public void emptyAndWhitespaceDefaultsBecomeNull() {
    assertNull(SnowflakeDatabaseMetaDataImpl.normalizeColumnDefaultValue("", false));
    assertNull(SnowflakeDatabaseMetaDataImpl.normalizeColumnDefaultValue("   ", false));
    assertNull(SnowflakeDatabaseMetaDataImpl.normalizeColumnDefaultValue("\t", true));
  }

  @Test
  public void unquotedNumericDefaultIsPreserved() {
    assertEquals("5", SnowflakeDatabaseMetaDataImpl.normalizeColumnDefaultValue("5", false));
    assertEquals("5", SnowflakeDatabaseMetaDataImpl.normalizeColumnDefaultValue("  5  ", false));
  }

  @Test
  public void wrappingQuotesAreStrippedWhenStringsAreNotQuoted() {
    assertEquals(
        "apples", SnowflakeDatabaseMetaDataImpl.normalizeColumnDefaultValue("'apples'", false));
    assertEquals(
        "apples", SnowflakeDatabaseMetaDataImpl.normalizeColumnDefaultValue("  'apples'  ", false));
    assertEquals("'", SnowflakeDatabaseMetaDataImpl.normalizeColumnDefaultValue("''''", false));
  }

  @Test
  public void wrappingQuotesAreKeptWhenStringsAreQuoted() {
    assertEquals(
        "'apples'", SnowflakeDatabaseMetaDataImpl.normalizeColumnDefaultValue("'apples'", true));
  }
}
