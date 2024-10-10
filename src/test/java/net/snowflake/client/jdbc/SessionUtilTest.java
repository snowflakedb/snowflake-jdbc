package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SessionUtil;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class SessionUtilTest {
  @Test
  public void testGetCommonParams() throws Exception {
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

    // Test unknown param name
    Map<String, Object> result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": true}]"));
    Assert.assertTrue((boolean) result.get("testParam"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": false}]"));
    Assert.assertFalse((boolean) result.get("testParam"));

    result =
        SessionUtil.getCommonParams(mapper.readTree("[{\"name\": \"testParam\", \"value\": 0}]"));
    Assert.assertEquals(0, (int) result.get("testParam"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": 1000}]"));
    Assert.assertEquals(1000, (int) result.get("testParam"));

    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": \"\"}]"));
    Assert.assertEquals("", result.get("testParam"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": \"value\"}]"));
    Assert.assertEquals("value", result.get("testParam"));

    // Test known param name
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"CLIENT_DISABLE_INCIDENTS\", \"value\": true}]"));
    Assert.assertTrue((boolean) result.get("CLIENT_DISABLE_INCIDENTS"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"CLIENT_DISABLE_INCIDENTS\", \"value\": false}]"));
    Assert.assertFalse((boolean) result.get("CLIENT_DISABLE_INCIDENTS"));

    result =
        SessionUtil.getCommonParams(
            mapper.readTree(
                "[{\"name\": \"CLIENT_STAGE_ARRAY_BINDING_THRESHOLD\", \"value\": 0}]"));
    Assert.assertEquals(0, (int) result.get("CLIENT_STAGE_ARRAY_BINDING_THRESHOLD"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree(
                "[{\"name\": \"CLIENT_STAGE_ARRAY_BINDING_THRESHOLD\", \"value\": 1000}]"));
    Assert.assertEquals(1000, (int) result.get("CLIENT_STAGE_ARRAY_BINDING_THRESHOLD"));

    result =
        SessionUtil.getCommonParams(mapper.readTree("[{\"name\": \"TIMEZONE\", \"value\": \"\"}]"));
    Assert.assertEquals("", result.get("TIMEZONE"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"TIMEZONE\", \"value\": \"value\"}]"));
    Assert.assertEquals("value", result.get("TIMEZONE"));
  }
}
