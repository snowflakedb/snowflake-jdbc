package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mockito;

public class ObjectMapperTest {
  private static final int jacksonDefaultMaxStringLength = 20_000_000;
  static String originalLogger;

  static class DataProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          Arguments.of(16 * 1024 * 1024, jacksonDefaultMaxStringLength),
          Arguments.of(16 * 1024 * 1024, 23_000_000),
          Arguments.of(32 * 1024 * 1024, 45_000_000),
          Arguments.of(64 * 1024 * 1024, 90_000_000),
          Arguments.of(128 * 1024 * 1024, 180_000_000));
    }
  }

  @BeforeAll
  public static void setProperty() {
    originalLogger = System.getProperty("net.snowflake.jdbc.loggerImpl");
    System.setProperty("net.snowflake.jdbc.loggerImpl", "net.snowflake.client.log.JDK14Logger");
  }

  @AfterAll
  public static void clearProperty() {
    if (originalLogger != null) {
      System.setProperty("net.snowflake.jdbc.loggerImpl", originalLogger);
    } else {
      System.clearProperty("net.snowflake.jdbc.loggerImpl");
    }
    System.clearProperty(ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM);
  }

  private static void setJacksonDefaultMaxStringLength(int maxJsonStringLength) {
    System.setProperty(
        ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM, Integer.toString(maxJsonStringLength));
  }

  @Test
  public void testInvalidMaxJsonStringLength() throws SQLException {
    System.setProperty(ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM, "abc");
    // calling getObjectMapper() should log the exception but not throw
    // default maxJsonStringLength value will be used
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    int stringLengthInMapper = mapper.getFactory().streamReadConstraints().getMaxStringLength();
    assertEquals(ObjectMapperFactory.DEFAULT_MAX_JSON_STRING_LEN, stringLengthInMapper);
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  public void testObjectMapperWithLargeJsonString(int lobSizeInBytes, int maxJsonStringLength) {
    setJacksonDefaultMaxStringLength(maxJsonStringLength);
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    try {
      JsonNode jsonNode = mapper.readTree(generateBase64EncodedJsonString(lobSizeInBytes));
      assertNotNull(jsonNode);
    } catch (Exception e) {
      // exception is expected when jackson's default maxStringLength value is used while retrieving
      // 16M string data
      assertEquals(jacksonDefaultMaxStringLength, maxJsonStringLength);
    }
  }

  // -------------------------------------------------------------------------
  // getObjectMapper() -- config flag assertions
  // -------------------------------------------------------------------------

  @Test
  public void testGetObjectMapper_enablesUseBigDecimalForFloats() {
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    assertTrue(mapper.isEnabled(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS));
  }

  @Test
  public void testGetObjectMapper_disablesAccessModifierOverrides() {
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    assertFalse(mapper.isEnabled(MapperFeature.OVERRIDE_PUBLIC_ACCESS_MODIFIERS));
    assertFalse(mapper.isEnabled(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS));
  }

  @Test
  public void testGetObjectMapper_appliesDefaultMaxStringLength() {
    System.clearProperty(ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM);
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    assertEquals(
        ObjectMapperFactory.DEFAULT_MAX_JSON_STRING_LEN,
        mapper.getFactory().streamReadConstraints().getMaxStringLength());
  }

  // -------------------------------------------------------------------------
  // getObjectMapperForSession() -- coverage of the session-aware path
  // -------------------------------------------------------------------------

  @Test
  public void testGetObjectMapperForSession_withDateFormat_serializesDate() throws Exception {
    ObjectMapper mapper = objectMapperForSession("YYYY-MM-DD");
    assertEquals(
        "\"2025-03-09\"", mapper.writeValueAsString(new Date(2025 - 1900, Calendar.MARCH, 9)));
  }

  @Test
  public void testGetObjectMapperForSession_withDateFormat_deserializesDate() throws Exception {
    ObjectMapper mapper = objectMapperForSession("YYYY-MM-DD");
    assertEquals(
        new Date(2025 - 1900, Calendar.MARCH, 9), mapper.readValue("\"2025-03-09\"", Date.class));
  }

  @Test
  public void testGetObjectMapperForSession_withDateTimeFormat_serializesDate() throws Exception {
    ObjectMapper mapper = objectMapperForSession("YYYY-MM-DD HH24:MI:SS");
    assertEquals(
        "\"2025-03-09 14:30:00\"",
        mapper.writeValueAsString(new Date(2025 - 1900, Calendar.MARCH, 9, 14, 30, 0)));
  }

  @Test
  public void testGetObjectMapperForSession_nullSession_returnsBaseMapper() {
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapperForSession(null);
    assertNotNull(mapper);
    assertTrue(mapper.isEnabled(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS));
  }

  @Test
  public void testGetObjectMapperForSession_nullParameters_returnsBaseMapper() {
    SFBaseSession session = Mockito.mock(SFBaseSession.class);
    Mockito.when(session.getCommonParameters()).thenReturn(null);

    ObjectMapper mapper = ObjectMapperFactory.getObjectMapperForSession(session);
    assertNotNull(mapper);
    assertTrue(mapper.isEnabled(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS));
  }

  @Test
  public void testGetObjectMapperForSession_missingDateOutputFormat_keepsTimestampsEnabled() {
    SFBaseSession session = Mockito.mock(SFBaseSession.class);
    Mockito.when(session.getCommonParameters()).thenReturn(new HashMap<>());

    ObjectMapper mapper = ObjectMapperFactory.getObjectMapperForSession(session);
    assertTrue(mapper.isEnabled(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS));
  }

  private static ObjectMapper objectMapperForSession(String dateOutputFormat) {
    SFBaseSession session = Mockito.mock(SFBaseSession.class);
    Map<String, Object> params = new HashMap<>();
    params.put("DATE_OUTPUT_FORMAT", dateOutputFormat);
    Mockito.when(session.getCommonParameters()).thenReturn(params);
    return ObjectMapperFactory.getObjectMapperForSession(session);
  }

  private String generateBase64EncodedJsonString(int numChar) {
    StringBuilder jsonStr = new StringBuilder();
    String largeStr = SnowflakeUtil.randomAlphaNumeric(numChar);

    // encode the string and put it into a JSON formatted string
    jsonStr.append("[\"").append(encodeStringToBase64(largeStr)).append("\"]");
    return jsonStr.toString();
  }

  private String encodeStringToBase64(String stringToBeEncoded) {
    return Base64.getEncoder().encodeToString(stringToBeEncoded.getBytes(StandardCharsets.UTF_8));
  }
}
