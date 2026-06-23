package net.snowflake.client.internal.api.implementation.connection;

import static net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl.needsStageQuoting;
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
  public void needsStageQuotingReturnsTrueForSpaces() {
    assertTrue(needsStageQuoting("@\"my stage\"/path/file.csv"));
  }

  @Test
  public void needsStageQuotingReturnsFalseForAlreadyQuoted() {
    assertFalse(needsStageQuoting("'@\"DB\".\"日本語\".STAGE'"));
    assertFalse(needsStageQuoting("$$@\"DB\".\"日本語\".STAGE$$"));
  }

  @Test
  public void quoteStageRefIfNeededQuotesNonAscii() {
    assertEquals("'@\"DB\".\"日本語\".STAGE'", quoteStageRefIfNeeded("@\"DB\".\"日本語\".STAGE"));
    assertEquals(
        "'@\"DQ\".\"自動化専用_変更禁止\".SNOWPARK_TEMP_STAGE_XYZ/prefix'",
        quoteStageRefIfNeeded("@\"DQ\".\"自動化専用_変更禁止\".SNOWPARK_TEMP_STAGE_XYZ/prefix"));
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
