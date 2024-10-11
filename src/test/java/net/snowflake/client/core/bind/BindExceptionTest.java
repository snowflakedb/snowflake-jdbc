package net.snowflake.client.core.bind;

import static org.junit.jupiter.api.Assertions.assertEquals;

import net.snowflake.client.jdbc.telemetry.TelemetryField;
import org.junit.jupiter.api.Test;

public class BindExceptionTest {

  @Test
  public void testBindExceptionType() {
    assertEquals(BindException.Type.SERIALIZATION.field, TelemetryField.FAILED_BIND_SERIALIZATION);
    assertEquals(BindException.Type.UPLOAD.field, TelemetryField.FAILED_BIND_UPLOAD);
    assertEquals(BindException.Type.OTHER.field, TelemetryField.FAILED_BIND_OTHER);
  }

  @Test
  public void testBindExceptionConstructor() {
    BindException exception = new BindException("testException", BindException.Type.SERIALIZATION);
    assertEquals(exception.getMessage(), "testException");
    assertEquals(exception.type.field, TelemetryField.FAILED_BIND_SERIALIZATION);
  }
}
