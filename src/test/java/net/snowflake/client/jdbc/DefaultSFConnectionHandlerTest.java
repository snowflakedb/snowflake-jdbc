package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.HttpHeadersCustomizer.HTTP_HEADER_CUSTOMIZERS_PROPERTY_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import net.snowflake.client.core.SFSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class DefaultSFConnectionHandlerTest {
  private DefaultSFConnectionHandler connectionHandler;
  private SnowflakeConnectString connectString;
  private SFSession mockSession;
  private Properties properties;
  private ArgumentCaptor<List<HttpHeadersCustomizer>> listCaptor;
  private HttpHeadersCustomizer mockCustomizer1;
  private HttpHeadersCustomizer mockCustomizer2;

  @BeforeEach
  void setUp() throws NoSuchFieldException, IllegalAccessException {
    listCaptor = ArgumentCaptor.forClass(List.class);
    mockCustomizer1 = mock();
    mockCustomizer2 = mock();
    mockSession = mock();

    Properties prop = new Properties();
    prop.put("account", "s3testaccount");
    prop.put("user", "test");
    prop.put("password", "test");
    connectString =
        SnowflakeConnectString.parse("jdbc:snowflake://testaccount.localhost:8080", prop);
    connectionHandler = new DefaultSFConnectionHandler(connectString, true);

    // Use Reflection to inject the mockSession into the private final field
    Field sessionField = DefaultSFConnectionHandler.class.getDeclaredField("sfSession");
    sessionField.setAccessible(true);
    sessionField.set(connectionHandler, mockSession);

    properties = new Properties();
  }

  @Test
  void testEmptyHttpHeaderCustomizersWhenPropertyNotSet() throws SQLException {
    connectionHandler.initialize(connectString, "", "", properties);
    verify(mockSession, never()).setHttpHeadersCustomizers(anyList());
  }

  @Test
  void testEmptyHttpHeaderCustomizersWhenIsNotList() throws SQLException {
    properties.put(HTTP_HEADER_CUSTOMIZERS_PROPERTY_KEY, "Not a List");
    connectionHandler.initialize(connectString, "", "", properties);
    verify(mockSession, never()).setHttpHeadersCustomizers(anyList());
  }

  @Test
  void testEmptyHttpHeaderCustomizersWhenEmptyListPassed() throws SQLException {
    properties.put(HTTP_HEADER_CUSTOMIZERS_PROPERTY_KEY, Collections.emptyList());
    connectionHandler.initialize(connectString, "", "", properties);
    verify(mockSession).setHttpHeadersCustomizers(listCaptor.capture());
    assertTrue(listCaptor.getValue().isEmpty());
  }

  @Test
  void testSetValidCustomizersFromProperty() throws SQLException {
    List<HttpHeadersCustomizer> inputList = Arrays.asList(mockCustomizer1, mockCustomizer2);
    properties.put(HTTP_HEADER_CUSTOMIZERS_PROPERTY_KEY, inputList);

    connectionHandler.initialize(connectString, "", "", properties);

    verify(mockSession).setHttpHeadersCustomizers(listCaptor.capture());
    List<HttpHeadersCustomizer> capturedList = listCaptor.getValue();
    assertEquals(2, capturedList.size());
    assertTrue(capturedList.contains(mockCustomizer1));
    assertTrue(capturedList.contains(mockCustomizer2));
  }

  @Test
  void testSetsOnlyValidHttpHeaderCustomizersWhenPropertyHasMixedTypes() throws SQLException {
    List<?> inputList =
        Arrays.asList("String", mockCustomizer1, null, new Object(), mockCustomizer2);
    properties.put(HTTP_HEADER_CUSTOMIZERS_PROPERTY_KEY, inputList);

    connectionHandler.initialize(connectString, "", "", properties);

    verify(mockSession).setHttpHeadersCustomizers(listCaptor.capture());
    List<HttpHeadersCustomizer> capturedList = listCaptor.getValue();
    assertEquals(2, capturedList.size());
    assertTrue(capturedList.contains(mockCustomizer1));
    assertTrue(capturedList.contains(mockCustomizer2));
    assertFalse(capturedList.stream().anyMatch(c -> !(c instanceof HttpHeadersCustomizer)));
  }

  @Test
  void testEmptyHttpHeaderCustomizersWhenPropertyHasOnlyInvalidTypes() throws SQLException {
    List<?> inputList = Arrays.asList("String", new Object(), null);
    properties.put(HTTP_HEADER_CUSTOMIZERS_PROPERTY_KEY, inputList);

    connectionHandler.initialize(connectString, "", "", properties);

    verify(mockSession).setHttpHeadersCustomizers(listCaptor.capture());
    assertTrue(listCaptor.getValue().isEmpty());
  }
}
