package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SessionUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SessionUtilTest {
  @Test
  public void testGetCommonParams() throws Exception {
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

    // Test unknown param name
    Map<String, Object> result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": true}]"));
    Assertions.assertTrue((boolean) result.get("testParam"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": false}]"));
    Assertions.assertFalse((boolean) result.get("testParam"));

    result =
        SessionUtil.getCommonParams(mapper.readTree("[{\"name\": \"testParam\", \"value\": 0}]"));
    Assertions.assertEquals(0, (int) result.get("testParam"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": 1000}]"));
    Assertions.assertEquals(1000, (int) result.get("testParam"));

    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": \"\"}]"));
    Assertions.assertEquals("", result.get("testParam"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": \"value\"}]"));
    Assertions.assertEquals("value", result.get("testParam"));

    // Test known param name
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"CLIENT_DISABLE_INCIDENTS\", \"value\": true}]"));
    Assertions.assertTrue((boolean) result.get("CLIENT_DISABLE_INCIDENTS"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"CLIENT_DISABLE_INCIDENTS\", \"value\": false}]"));
    Assertions.assertFalse((boolean) result.get("CLIENT_DISABLE_INCIDENTS"));

    result =
        SessionUtil.getCommonParams(
            mapper.readTree(
                "[{\"name\": \"CLIENT_STAGE_ARRAY_BINDING_THRESHOLD\", \"value\": 0}]"));
    Assertions.assertEquals(0, (int) result.get("CLIENT_STAGE_ARRAY_BINDING_THRESHOLD"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree(
                "[{\"name\": \"CLIENT_STAGE_ARRAY_BINDING_THRESHOLD\", \"value\": 1000}]"));
    Assertions.assertEquals(1000, (int) result.get("CLIENT_STAGE_ARRAY_BINDING_THRESHOLD"));

    result =
        SessionUtil.getCommonParams(mapper.readTree("[{\"name\": \"TIMEZONE\", \"value\": \"\"}]"));
    Assertions.assertEquals("", result.get("TIMEZONE"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"TIMEZONE\", \"value\": \"value\"}]"));
    Assertions.assertEquals("value", result.get("TIMEZONE"));
  }
}
