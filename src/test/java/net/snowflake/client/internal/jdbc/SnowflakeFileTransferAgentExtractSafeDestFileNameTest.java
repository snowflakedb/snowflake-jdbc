package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import net.snowflake.client.api.exception.SnowflakeSQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link SnowflakeFileTransferAgent#extractSafeDestFileName(String, String)}.
 *
 * <p>Covers the SNOW-3437355 regression: server-supplied stage object keys must not be allowed to
 * escape the user's download directory via Windows backslash path separators or other traversal
 * forms.
 */
public class SnowflakeFileTransferAgentExtractSafeDestFileNameTest {

  private static final String QUERY_ID = "test-query-id";

  @Test
  public void simpleNameIsReturnedAsIs() throws SnowflakeSQLException {
    assertEquals(
        "report.csv", SnowflakeFileTransferAgent.extractSafeDestFileName("report.csv", QUERY_ID));
  }

  @Test
  public void forwardSlashPathReturnsBasename() throws SnowflakeSQLException {
    assertEquals(
        "report.csv",
        SnowflakeFileTransferAgent.extractSafeDestFileName("stage/dir/sub/report.csv", QUERY_ID));
  }

  @Test
  public void backslashOnlyPathReturnsBasename() throws SnowflakeSQLException {
    assertEquals(
        "evil.dll",
        SnowflakeFileTransferAgent.extractSafeDestFileName("stage\\dir\\evil.dll", QUERY_ID));
  }

  @Test
  public void mixedSeparatorsReturnTrailingBasename() throws SnowflakeSQLException {
    assertEquals(
        "real.csv",
        SnowflakeFileTransferAgent.extractSafeDestFileName("stage/dir\\sub/real.csv", QUERY_ID));
    assertEquals(
        "real.csv",
        SnowflakeFileTransferAgent.extractSafeDestFileName("stage\\dir/sub\\real.csv", QUERY_ID));
  }

  @Test
  public void windowsTraversalAttackIsNeutralized() throws SnowflakeSQLException {
    assertEquals(
        "evil.dll",
        SnowflakeFileTransferAgent.extractSafeDestFileName(
            "stage/dir/..\\..\\..\\Windows\\System32\\evil.dll", QUERY_ID));
  }

  @Test
  public void singleBackslashTraversalIsNeutralized() throws SnowflakeSQLException {
    assertEquals(
        "evil", SnowflakeFileTransferAgent.extractSafeDestFileName("stage/dir/..\\evil", QUERY_ID));
  }

  @Test
  public void trailingDotDotIsRejected() {
    assertThrows(
        SnowflakeSQLException.class,
        () -> SnowflakeFileTransferAgent.extractSafeDestFileName("stage/dir/..", QUERY_ID));
  }

  @Test
  public void backslashTrailingDotDotIsRejected() {
    assertThrows(
        SnowflakeSQLException.class,
        () -> SnowflakeFileTransferAgent.extractSafeDestFileName("stage\\dir\\..", QUERY_ID));
  }

  @ParameterizedTest
  @ValueSource(strings = {".", "..", ""})
  public void dotAndEmptyTokensAreRejected(String input) {
    assertThrows(
        SnowflakeSQLException.class,
        () -> SnowflakeFileTransferAgent.extractSafeDestFileName(input, QUERY_ID));
  }

  @Test
  public void trailingForwardSlashYieldsEmptyAndIsRejected() {
    assertThrows(
        SnowflakeSQLException.class,
        () -> SnowflakeFileTransferAgent.extractSafeDestFileName("stage/dir/", QUERY_ID));
  }

  @Test
  public void trailingBackslashYieldsEmptyAndIsRejected() {
    assertThrows(
        SnowflakeSQLException.class,
        () -> SnowflakeFileTransferAgent.extractSafeDestFileName("stage\\dir\\", QUERY_ID));
  }

  @Test
  public void nulByteIsRejected() {
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            SnowflakeFileTransferAgent.extractSafeDestFileName(
                "stage/dir/file\u0000hidden", QUERY_ID));
  }

  @Test
  public void nullSourceFileIsRejected() {
    assertThrows(
        SnowflakeSQLException.class,
        () -> SnowflakeFileTransferAgent.extractSafeDestFileName(null, QUERY_ID));
  }

  @Test
  public void leadingDotsInOtherwiseValidNamesAreAllowed() throws SnowflakeSQLException {
    assertEquals(
        ".hidden",
        SnowflakeFileTransferAgent.extractSafeDestFileName("stage/dir/.hidden", QUERY_ID));
    assertEquals(
        "..something",
        SnowflakeFileTransferAgent.extractSafeDestFileName("stage/dir/..something", QUERY_ID));
  }

  @Test
  public void noSeparatorAtAllReturnsWholeString() throws SnowflakeSQLException {
    assertEquals(
        "lonely.csv", SnowflakeFileTransferAgent.extractSafeDestFileName("lonely.csv", QUERY_ID));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "C:foo",
        ":foo",
        "foo:",
        "passwd:hidden",
        "stage/dir/C:foo",
        "stage\\dir\\name:stream"
      })
  public void colonInBasenameIsRejected(String input) {
    assertThrows(
        SnowflakeSQLException.class,
        () -> SnowflakeFileTransferAgent.extractSafeDestFileName(input, QUERY_ID));
  }
}
