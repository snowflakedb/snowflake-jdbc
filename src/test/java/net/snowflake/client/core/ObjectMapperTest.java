package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ObjectMapperTest {
  @Parameterized.Parameters(name = "LOBSize={0}, maxJsonStringLength={1}")
  public static Collection<Object[]> data() {
    int[] lobSizeInMB = new int[] {16, 16, 32, 64, 128};
    // maxJsonStringLength to be set for the corresponding LOB size
    int[] maxJsonStringLengths =
        new int[] {20_000_000, 23_000_000, 45_000_000, 90_000_000, 180_000_000};
    List<Object[]> ret = new ArrayList<>();
    for (int i = 0; i < lobSizeInMB.length; i++) {
      ret.add(new Object[] {lobSizeInMB[i], maxJsonStringLengths[i]});
    }
    return ret;
  }

  private final int lobSizeInBytes;
  private final int maxJsonStringLength;
  private final int jacksonDefaultMaxStringLength = 20_000_000;

  @After
  public void clearProperty() {
    System.clearProperty(ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM);
  }

  public ObjectMapperTest(int lobSizeInMB, int maxJsonStringLength) {
    // convert LOB size from MB to bytes
    this.lobSizeInBytes = lobSizeInMB * 1024 * 1024;
    this.maxJsonStringLength = maxJsonStringLength;
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
    Assert.assertEquals(ObjectMapperFactory.DEFAULT_MAX_JSON_STRING_LEN, stringLengthInMapper);
  }

  @Test
  public void testObjectMapperWithLargeJsonString() {
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    try {
      JsonNode jsonNode = mapper.readTree(generateBase64EncodedJsonString(lobSizeInBytes));
      Assert.assertNotNull(jsonNode);
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
