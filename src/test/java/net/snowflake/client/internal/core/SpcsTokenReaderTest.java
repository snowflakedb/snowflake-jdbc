package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class SpcsTokenReaderTest {

  /**
   * Per the design spec: when {@code SNOWFLAKE_RUNNING_INSIDE_SPCS} is unset (or empty per Node.js
   * {@code !process.env[…]} parity), the driver MUST NOT attempt to access the token file.
   */
  @Test
  public void shouldReturnNullWhenNotRunningInsideSpcs() {
    SpcsTokenReader reader = spy(new SpcsTokenReader());
    doReturn(false).when(reader).isRunningInsideSpcs();

    assertNull(reader.readSpcsToken());
  }

  @Test
  public void shouldReturnTrimmedTokenWhenRunningInsideSpcsAndFilePresent() throws IOException {
    SpcsTokenReader reader = spy(new SpcsTokenReader());
    doReturn(true).when(reader).isRunningInsideSpcs();
    doReturn("  \n\tservice-token-value\n  ".getBytes(StandardCharsets.UTF_8))
        .when(reader)
        .readTokenFileBytes();

    assertEquals("service-token-value", reader.readSpcsToken());
  }

  @Test
  public void shouldReadFileAsUtf8() throws IOException {
    String value = "tókén";
    SpcsTokenReader reader = spy(new SpcsTokenReader());
    doReturn(true).when(reader).isRunningInsideSpcs();
    doReturn(value.getBytes(StandardCharsets.UTF_8)).when(reader).readTokenFileBytes();

    assertEquals(value, reader.readSpcsToken());
  }

  @Test
  public void shouldReturnNullForBlankFile() throws IOException {
    SpcsTokenReader reader = spy(new SpcsTokenReader());
    doReturn(true).when(reader).isRunningInsideSpcs();
    doReturn("  \n\t \n".getBytes(StandardCharsets.UTF_8)).when(reader).readTokenFileBytes();

    assertNull(reader.readSpcsToken());
  }

  @Test
  public void shouldReturnNullAndNotThrowOnIoError() throws IOException {
    SpcsTokenReader reader = spy(new SpcsTokenReader());
    doReturn(true).when(reader).isRunningInsideSpcs();
    doThrow(new IOException("boom")).when(reader).readTokenFileBytes();

    assertNull(reader.readSpcsToken());
  }

  @Test
  public void productionConstantsMatchDesignSpec() {
    assertEquals("SNOWFLAKE_RUNNING_INSIDE_SPCS", SpcsTokenReader.SPCS_RUNNING_INSIDE_ENV_VAR);
    assertEquals("/snowflake/session/spcs_token", SpcsTokenReader.SPCS_TOKEN_FILE_PATH);
  }
}
