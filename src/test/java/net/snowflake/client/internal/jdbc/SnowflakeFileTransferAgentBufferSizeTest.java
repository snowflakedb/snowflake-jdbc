package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.core.SFSessionProperty;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SnowflakeFileTransferAgentBufferSizeTest {

  @Test
  public void returnsDefaultWhenSessionIsNull() {
    assertEquals(
        SnowflakeFileTransferAgent.MAX_BUFFER_SIZE,
        SnowflakeFileTransferAgent.resolveMaxBufferSize(null));
  }

  @Test
  public void returnsDefaultWhenPropertyAbsent() {
    SFBaseSession session = Mockito.mock(SFBaseSession.class);
    Mockito.when(session.getConnectionPropertiesMap())
        .thenReturn(new HashMap<SFSessionProperty, Object>());
    assertEquals(
        SnowflakeFileTransferAgent.MAX_BUFFER_SIZE,
        SnowflakeFileTransferAgent.resolveMaxBufferSize(session));
  }

  @Test
  public void returnsConfiguredValue() {
    SFBaseSession session = Mockito.mock(SFBaseSession.class);
    Map<SFSessionProperty, Object> props = new HashMap<>();
    int configured = 64 * 1024 * 1024;
    props.put(SFSessionProperty.PUT_GET_MAX_BUFFER_SIZE, configured);
    Mockito.when(session.getConnectionPropertiesMap()).thenReturn(props);
    assertEquals(
        configured, SnowflakeFileTransferAgent.resolveMaxBufferSize(session));
  }

  @Test
  public void ignoresNonPositiveValue() {
    SFBaseSession session = Mockito.mock(SFBaseSession.class);
    Map<SFSessionProperty, Object> props = new HashMap<>();
    props.put(SFSessionProperty.PUT_GET_MAX_BUFFER_SIZE, 0);
    Mockito.when(session.getConnectionPropertiesMap()).thenReturn(props);
    assertEquals(
        SnowflakeFileTransferAgent.MAX_BUFFER_SIZE,
        SnowflakeFileTransferAgent.resolveMaxBufferSize(session));
  }
}

