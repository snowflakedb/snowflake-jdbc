package net.snowflake.client.internal.api.implementation.metadata;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;
import net.snowflake.common.util.Wildcard;
import org.junit.jupiter.api.Test;

public class SnowflakeDatabaseMetaDataImplSchemaFilterTest {

  private static Pattern compile(String schemaPattern) {
    return Wildcard.toRegexPattern(schemaPattern, true);
  }

  @Test
  public void nullCompiledPatternAcceptsAnySchema() {
    assertTrue(SnowflakeDatabaseMetaDataImpl.matchesSchemaName(null, "ANY_SCHEMA", false));
  }

  @Test
  public void compiledPatternMatchIsAccepted() {
    Pattern compiled = compile("FOO%");
    assertTrue(SnowflakeDatabaseMetaDataImpl.matchesSchemaName(compiled, "FOOBAR", false));
  }

  @Test
  public void compiledPatternMismatchIsRejectedWhenNotExactSchema() {
    Pattern compiled = compile("FOO");
    assertFalse(SnowflakeDatabaseMetaDataImpl.matchesSchemaName(compiled, "BAR", false));
  }

  @Test
  public void exactSchemaAcceptsShowRowsEvenWhenCompiledPatternDoesNotMatch() {
    // SHOW may quote names that contain % (e.g. "FOO%BAR"), which does not match the LIKE regex
    // compiled from the unquoted session schema. Exact-schema mode trusts the SHOW scoping.
    Pattern compiled = compile("FOO%BAR");
    assertTrue(SnowflakeDatabaseMetaDataImpl.matchesSchemaName(compiled, "\"FOO%BAR\"", true));
  }
}
