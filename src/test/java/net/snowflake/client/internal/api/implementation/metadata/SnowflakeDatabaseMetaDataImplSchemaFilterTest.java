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
    assertTrue(SnowflakeDatabaseMetaDataImpl.matchesSchemaName(null, "ANY_SCHEMA", false, null));
  }

  @Test
  public void compiledPatternMatchIsAccepted() {
    Pattern compiled = compile("FOO%");
    assertTrue(SnowflakeDatabaseMetaDataImpl.matchesSchemaName(compiled, "FOOBAR", false, "FOO%"));
  }

  @Test
  public void compiledPatternMismatchIsRejectedWhenNotExactSchema() {
    Pattern compiled = compile("FOO");
    assertFalse(SnowflakeDatabaseMetaDataImpl.matchesSchemaName(compiled, "BAR", false, "FOO"));
  }

  @Test
  public void exactSchemaAcceptsLiteralNameEqualityWhenPatternDoesNotMatch() {
    // A schema name containing regex metacharacters will not match the compiled wildcard
    // pattern, but exact-schema mode should still accept a literal equals.
    String schema = "FOO.BAR";
    Pattern compiled = compile(schema);
    assertTrue(SnowflakeDatabaseMetaDataImpl.matchesSchemaName(compiled, schema, true, schema));
  }

  @Test
  public void exactSchemaDoesNotAcceptUnrelatedSchemaWhenPatternDoesNotMatch() {
    Pattern compiled = compile("FOO");
    assertFalse(SnowflakeDatabaseMetaDataImpl.matchesSchemaName(compiled, "BAR", true, "FOO"));
  }
}
