package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeUtil.createCaseInsensitiveMap;
import static net.snowflake.client.jdbc.SnowflakeUtil.extractColumnMetadata;
import static net.snowflake.client.jdbc.SnowflakeUtil.getSnowflakeType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.ObjectMapperFactory;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class SnowflakeUtilTest extends BaseJDBCTest {

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();

  @Test
  public void testCreateMetadata() throws Throwable {
    // given
    ObjectNode rootNode = createRootNode();
    ArrayNode fields = OBJECT_MAPPER.createArrayNode();
    JsonNode fieldOne = createFieldNode("name1", null, 256, null, "text", false, "collation", 256);
    fields.add(fieldOne);
    JsonNode fieldTwo = createFieldNode("name2", 5, 128, 2, "real", true, "collation", 256);
    fields.add(fieldTwo);
    rootNode.putIfAbsent("fields", fields);
    SnowflakeColumnMetadata expectedColumnMetadata =
        createExpectedMetadata(rootNode, fieldOne, fieldTwo);
    // when
    SnowflakeColumnMetadata columnMetadata = extractColumnMetadata(rootNode, false, null);
    // then
    assertNotNull(columnMetadata);
    assertEquals(
        OBJECT_MAPPER.writeValueAsString(expectedColumnMetadata),
        OBJECT_MAPPER.writeValueAsString(columnMetadata));
  }

  @Test
  public void testCreateFieldsMetadataForObject() throws Throwable {
    // given
    ObjectNode rootNode = createRootNode();
    ArrayNode fields = OBJECT_MAPPER.createArrayNode();
    fields.add(
        OBJECT_MAPPER.readTree(
            "{\"fieldName\":\"name1\", \"fieldType\": {\"type\":\"text\",\"precision\":null,\"length\":256,\"scale\":null,\"nullable\":false}}"));
    fields.add(
        OBJECT_MAPPER.readTree(
            "{\"fieldName\":\"name2\", \"fieldType\": {\"type\":\"real\",\"precision\":5,\"length\":128,\"scale\":null,\"nullable\":true}}"));
    rootNode.putIfAbsent("fields", fields);

    // when
    SnowflakeColumnMetadata columnMetadata = extractColumnMetadata(rootNode, false, null);
    // then
    assertNotNull(columnMetadata);
    assertEquals("OBJECT", columnMetadata.getTypeName());

    FieldMetadata firstField = columnMetadata.getFields().get(0);
    assertEquals("name1", firstField.getName());
    assertEquals(SnowflakeType.TEXT, firstField.getBase());
    assertEquals(256, firstField.getByteLength());
    assertFalse(firstField.isNullable());

    FieldMetadata secondField = columnMetadata.getFields().get(1);
    assertEquals("name2", secondField.getName());
    assertEquals(SnowflakeType.REAL, secondField.getBase());
    assertEquals(128, secondField.getByteLength());
    assertEquals(5, secondField.getPrecision());
    assertTrue(secondField.isNullable());
  }

  @Test
  public void shouldConvertCreateCaseInsensitiveMap() {
    Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");

    map = SnowflakeUtil.createCaseInsensitiveMap(map);
    assertTrue(map instanceof TreeMap);
    assertEquals(String.CASE_INSENSITIVE_ORDER, ((TreeMap<String, String>) map).comparator());
    assertEquals("value1", map.get("key1"));
    assertEquals("value1", map.get("Key1"));
    assertEquals("value1", map.get("KEy1"));

    map.put("KEY1", "changed_value1");
    assertEquals("changed_value1", map.get("KEY1"));
  }

  @Test
  public void shouldConvertHeadersCreateCaseInsensitiveMap() {
    Header[] headers =
        new Header[] {new BasicHeader("key1", "value1"), new BasicHeader("key2", "value2")};

    Map<String, String> map = createCaseInsensitiveMap(headers);
    assertTrue(map instanceof TreeMap);
    assertEquals(String.CASE_INSENSITIVE_ORDER, ((TreeMap<String, String>) map).comparator());
    assertEquals("value1", map.get("key1"));
    assertEquals("value2", map.get("key2"));
    assertEquals("value1", map.get("Key1"));
    assertEquals("value2", map.get("Key2"));
  }

  private static SnowflakeColumnMetadata createExpectedMetadata(
      JsonNode rootNode, JsonNode fieldOne, JsonNode fieldTwo) throws SnowflakeSQLLoggedException {
    ColumnTypeInfo columnTypeInfo =
        getSnowflakeType(rootNode.path("type").asText(), null, null, null, 0, true, false);
    ColumnTypeInfo columnTypeInfoNodeOne =
        getSnowflakeType(
            fieldOne.path("type").asText(), null, null, null, Types.BIGINT, true, false);
    ColumnTypeInfo columnTypeInfoNodeTwo =
        getSnowflakeType(
            fieldTwo.path("type").asText(), null, null, null, Types.DECIMAL, true, false);
    SnowflakeColumnMetadata expectedColumnMetadata =
        new SnowflakeColumnMetadata(
            rootNode.path("name").asText(),
            columnTypeInfo.getColumnType(),
            rootNode.path("nullable").asBoolean(),
            rootNode.path("length").asInt(),
            rootNode.path("precision").asInt(),
            rootNode.path("scale").asInt(),
            columnTypeInfo.getExtColTypeName(),
            false,
            columnTypeInfo.getSnowflakeType(),
            Arrays.asList(
                new FieldMetadata(
                    fieldOne.path("name").asText(),
                    columnTypeInfoNodeOne.getExtColTypeName(),
                    columnTypeInfoNodeOne.getColumnType(),
                    fieldOne.path("nullable").asBoolean(),
                    fieldOne.path("length").asInt(),
                    fieldOne.path("precision").asInt(),
                    fieldOne.path("scale").asInt(),
                    fieldOne.path("fixed").asBoolean(),
                    columnTypeInfoNodeOne.getSnowflakeType(),
                    new ArrayList<>()),
                new FieldMetadata(
                    fieldTwo.path("name").asText(),
                    columnTypeInfoNodeTwo.getExtColTypeName(),
                    columnTypeInfoNodeTwo.getColumnType(),
                    fieldTwo.path("nullable").asBoolean(),
                    fieldTwo.path("length").asInt(),
                    fieldTwo.path("precision").asInt(),
                    fieldTwo.path("scale").asInt(),
                    fieldTwo.path("fixed").asBoolean(),
                    columnTypeInfoNodeTwo.getSnowflakeType(),
                    new ArrayList<>())),
            rootNode.path("database").asText(),
            rootNode.path("schema").asText(),
            rootNode.path("table").asText(),
            false,
            rootNode.path("dimension").asInt());
    return expectedColumnMetadata;
  }

  private static ObjectNode createRootNode() {
    ObjectNode rootNode = createFieldNode("STRUCT", 2, 128, 8, "object", false, null, 42);
    rootNode.put("database", "databaseName");
    rootNode.put("schema", "schemaName");
    rootNode.put("table", "tableName");
    return rootNode;
  }

  private static ObjectNode createFieldNode(
      String name,
      Integer precision,
      Integer byteLength,
      Integer scale,
      String type,
      boolean nullable,
      String collation,
      Integer length) {
    ObjectNode fieldNode = OBJECT_MAPPER.createObjectNode();
    fieldNode.put("name", name);
    fieldNode.put("type", type);
    fieldNode.put("precision", precision);
    fieldNode.put("byteLength", byteLength);
    fieldNode.put("scale", scale);
    fieldNode.put("type", type);
    fieldNode.put("nullable", nullable);
    fieldNode.put("collation", collation);
    fieldNode.put("length", length);
    return fieldNode;
  }
}
