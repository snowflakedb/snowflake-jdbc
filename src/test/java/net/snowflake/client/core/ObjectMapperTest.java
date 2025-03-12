package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Base64;
import java.util.stream.Stream;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

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
