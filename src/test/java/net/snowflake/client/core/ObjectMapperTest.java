package net.snowflake.client.core;

import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
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
    // LOB sizes in MB
    int[] lobSizes = new int[] {16, 32, 64, 128};
    // maxJsonStringLength to be set for the corresponding LOB size
    int[] maxJsonStringLengths = new int[] {23_000_000, 45_000_000, 90_000_000, 180_000_000};
    List<Object[]> ret = new ArrayList<>();
    for (int i = 0; i < lobSizes.length; i++) {
      ret.add(new Object[] {lobSizes[i], maxJsonStringLengths[i]});
    }
    return ret;
  }

  private final int lobSize;

  @After
  public void clearProperty() {
    System.clearProperty("net.snowflake.jdbc.objectMapper.maxJsonStringLength");
  }

  public ObjectMapperTest(int lobSize, int maxLength) {
    // convert LOB size from MB to bytes
    this.lobSize = lobSize * 1024 * 1024;
    System.setProperty(
        "net.snowflake.jdbc.objectMapper.maxJsonStringLength", Integer.toString(maxLength));
  }

  @Test
  public void testObjectMapperWithLargeJsonString() {
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    try {
      JsonNode jsonNode = mapper.readTree(generateBase64EncodedJsonString(lobSize));
      Assert.assertNotNull(jsonNode);
    } catch (Exception e) {
      fail("failed to read the JsonNode. err: " + e.getMessage());
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
