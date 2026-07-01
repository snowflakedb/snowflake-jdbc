package net.snowflake.client.internal.api.implementation.connection;

import static net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl.buildStageFileRef;
import static net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl.needsStageQuoting;
import static net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl.normalizeStageNameForRef;
import static net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl.quoteStageRefIfNeeded;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Unit tests for stage-reference quoting helpers in {@link SnowflakeConnectionImpl}. */
public class StageQuotingTest {

  @Test
  public void needsStageQuotingReturnsFalseForAsciiRefs() {
    assertFalse(needsStageQuoting("@stage"));
    assertFalse(needsStageQuoting("@stage/dir/file.csv"));
    assertFalse(needsStageQuoting("@~/dir/file.csv"));
    assertFalse(needsStageQuoting("@\"DB\".\"SCHEMA\".\"STAGE\""));
    assertFalse(needsStageQuoting("@db.schema.%table/dir"));
    assertFalse(needsStageQuoting(null));
  }

  @Test
  public void needsStageQuotingReturnsTrueForNonAscii() {
    assertTrue(needsStageQuoting("@\"DB\".\"日本語\".STAGE"));
    assertTrue(needsStageQuoting("@\"DQ\".\"自動化専用_変更禁止\".SNOWPARK_TEMP_STAGE_XYZ"));
    assertTrue(needsStageQuoting("@\"café\".STAGE"));
  }

  @Test
  public void needsStageQuotingReturnsTrueForSpacesAndParens() {
    assertTrue(needsStageQuoting("@\"my stage\"/path/file.csv"));
    assertTrue(needsStageQuoting("@\"ice cream (nice)\"/hello.txt"));
  }

  @Test
  public void needsStageQuotingReturnsFalseForAlreadyQuoted() {
    assertFalse(needsStageQuoting("'@\"DB\".\"日本語\".STAGE'"));
    assertFalse(needsStageQuoting("$$@\"DB\".\"日本語\".STAGE$$"));
  }

  @Test
  public void needsStageQuotingReturnsTrueForMalformedQuotedRef() {
    assertTrue(needsStageQuoting("'a'b'c'"));
  }

  @Test
  public void normalizeStageNameForRefStripsOuterQuotes() {
    assertEquals("@stage", normalizeStageNameForRef("stage"));
    assertEquals("@stage", normalizeStageNameForRef("@stage"));
    assertEquals("@%\"ice cream (nice)\"", normalizeStageNameForRef("'@%\"ice cream (nice)\"'"));
    assertEquals("@%\"ice cream (nice)\"", normalizeStageNameForRef("$$@%\"ice cream (nice)\"$$"));
    assertEquals("@stage_O'Brien", normalizeStageNameForRef("'@stage_O''Brien'"));
  }

  @Test
  public void buildStageFileRefHandlesPreQuotedStageName() {
    assertEquals("@stage/file.csv", buildStageFileRef("@stage", "file.csv"));
    assertEquals("@stage/file.csv", buildStageFileRef("@stage", "/file.csv"));
    assertEquals(
        "@%\"ice cream (nice)\"/hello.txt",
        buildStageFileRef("'@%\"ice cream (nice)\"'", "hello.txt"));
    assertEquals(
        "'@%\"ice cream (nice)\"/hello.txt'",
        quoteStageRefIfNeeded(buildStageFileRef("'@%\"ice cream (nice)\"'", "hello.txt")));
  }

  @Test
  public void quoteStageRefIfNeededQuotesNonAscii() {
    assertEquals("'@\"DB\".\"日本語\".STAGE'", quoteStageRefIfNeeded("@\"DB\".\"日本語\".STAGE"));
    assertEquals(
        "'@\"DQ\".\"自動化専用_変更禁止\".SNOWPARK_TEMP_STAGE_XYZ/file.txt'",
        quoteStageRefIfNeeded("@\"DQ\".\"自動化専用_変更禁止\".SNOWPARK_TEMP_STAGE_XYZ/file.txt"));
  }

  @Test
  public void quoteStageRefIfNeededLeavesAsciiUnchanged() {
    assertEquals("@stage", quoteStageRefIfNeeded("@stage"));
    assertEquals("@stage/dir/file.csv", quoteStageRefIfNeeded("@stage/dir/file.csv"));
    assertEquals("@\"DB\".\"SCHEMA\".STAGE", quoteStageRefIfNeeded("@\"DB\".\"SCHEMA\".STAGE"));
  }

  @Test
  public void quoteStageRefIfNeededLeavesAlreadyQuotedUnchanged() {
    String singleQuoted = "'@\"DB\".\"日本語\".STAGE'";
    assertEquals(singleQuoted, quoteStageRefIfNeeded(singleQuoted));
    String dollarQuoted = "$$@\"DB\".\"日本語\".STAGE$$";
    assertEquals(dollarQuoted, quoteStageRefIfNeeded(dollarQuoted));
  }

  @Test
  public void quoteStageRefIfNeededEscapesEmbeddedSingleQuotes() {
    assertEquals("'@stage_O''Brien'", quoteStageRefIfNeeded("@stage_O'Brien"));
    assertEquals("'@\"sch''ema\".STAGE'", quoteStageRefIfNeeded("@\"sch'ema\".STAGE"));
  }

  @Test
  public void quoteStageRefIfNeededHandlesNull() {
    assertNull(quoteStageRefIfNeeded(null));
  }
}
