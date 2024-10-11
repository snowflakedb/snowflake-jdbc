package net.snowflake.client.core.bind;

import net.snowflake.client.jdbc.telemetry.TelemetryField;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BindExceptionTest {

  @Test
  public void testBindExceptionType() {
    Assertions.assertEquals(
        BindException.Type.SERIALIZATION.field, TelemetryField.FAILED_BIND_SERIALIZATION);
    Assertions.assertEquals(BindException.Type.UPLOAD.field, TelemetryField.FAILED_BIND_UPLOAD);
    Assertions.assertEquals(BindException.Type.OTHER.field, TelemetryField.FAILED_BIND_OTHER);
  }

  @Test
  public void testBindExceptionConstructor() {
    BindException exception = new BindException("testException", BindException.Type.SERIALIZATION);
    Assertions.assertEquals(exception.getMessage(), "testException");
    Assertions.assertEquals(exception.type.field, TelemetryField.FAILED_BIND_SERIALIZATION);
  }
}
