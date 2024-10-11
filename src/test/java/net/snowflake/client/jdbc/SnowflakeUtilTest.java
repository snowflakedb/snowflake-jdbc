/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeUtil.getSnowflakeType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;

import net.snowflake.client.core.ObjectMapperFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

//@Category(TestCategoryCore.class)
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
    SnowflakeColumnMetadata columnMetadata =
        SnowflakeUtil.extractColumnMetadata(rootNode, false, null);
    // then
    Assertions.assertNotNull(columnMetadata);
    Assertions.assertEquals(OBJECT_MAPPER.writeValueAsString(expectedColumnMetadata), OBJECT_MAPPER.writeValueAsString(columnMetadata));
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
    SnowflakeColumnMetadata columnMetadata =
        SnowflakeUtil.extractColumnMetadata(rootNode, false, null);
    // then
    Assertions.assertNotNull(columnMetadata);
    Assertions.assertEquals("OBJECT", columnMetadata.getTypeName());

    FieldMetadata firstField = columnMetadata.getFields().get(0);
    Assertions.assertEquals("name1", firstField.getName());
    Assertions.assertEquals(SnowflakeType.TEXT, firstField.getBase());
    Assertions.assertEquals(256, firstField.getByteLength());
    Assertions.assertFalse(firstField.isNullable());

    FieldMetadata secondField = columnMetadata.getFields().get(1);
    Assertions.assertEquals("name2", secondField.getName());
    Assertions.assertEquals(SnowflakeType.REAL, secondField.getBase());
    Assertions.assertEquals(128, secondField.getByteLength());
    Assertions.assertEquals(5, secondField.getPrecision());
    Assertions.assertTrue(secondField.isNullable());
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
