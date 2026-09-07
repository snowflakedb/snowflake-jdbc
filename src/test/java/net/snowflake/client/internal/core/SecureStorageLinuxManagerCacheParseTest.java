package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class SecureStorageLinuxManagerCacheParseTest {

  private static final ObjectMapper MAPPER = ObjectMapperFactory.getObjectMapper();

  @Test
  public void validTokensObjectIsLoaded() {
    ObjectNode tokens = MAPPER.createObjectNode();
    tokens.put("host:user:ID_TOKEN", "secret");
    ObjectNode root = MAPPER.createObjectNode();
    root.set("tokens", tokens);

    Map<String, Map<String, String>> cache =
        SecureStorageLinuxManager.getInstance().readJsonStoreCache(root);

    assertEquals("secret", cache.get("tokens").get("host:user:ID_TOKEN"));
  }

  @Test
  public void missingTokensYieldsEmptyMap() {
    ObjectNode root = MAPPER.createObjectNode();

    Map<String, Map<String, String>> cache =
        SecureStorageLinuxManager.getInstance().readJsonStoreCache(root);

    assertTrue(cache.get("tokens").isEmpty());
  }

  @Test
  public void nonObjectTokensDoesNotThrowAndYieldsEmptyMap() {
    ObjectNode stringTokens = MAPPER.createObjectNode();
    stringTokens.set("tokens", TextNode.valueOf("not-an-object"));

    Map<String, Map<String, String>> fromString =
        SecureStorageLinuxManager.getInstance().readJsonStoreCache(stringTokens);
    assertTrue(fromString.get("tokens").isEmpty());

    ArrayNode array = MAPPER.createArrayNode();
    array.add("token");
    ObjectNode arrayTokens = MAPPER.createObjectNode();
    arrayTokens.set("tokens", array);

    Map<String, Map<String, String>> fromArray =
        SecureStorageLinuxManager.getInstance().readJsonStoreCache(arrayTokens);
    assertTrue(fromArray.get("tokens").isEmpty());
  }

  @Test
  public void nonObjectRootYieldsEmptyCache() {
    Map<String, Map<String, String>> cache =
        SecureStorageLinuxManager.getInstance().readJsonStoreCache(TextNode.valueOf("oops"));
    assertTrue(cache.isEmpty());
  }
}
