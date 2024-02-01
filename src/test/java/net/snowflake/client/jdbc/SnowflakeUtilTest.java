/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeUtil.getSnowflakeType;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.category.TestCategoryCore;
import net.snowflake.client.core.ObjectMapperFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Types;

@Category(TestCategoryCore.class)
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
    rootNode.put("fields", fields);
    SnowflakeColumnMetadata expectedColumnMetadata =
        createExpectedMetadata(rootNode, fieldOne, fieldTwo);
    // when
    SnowflakeColumnMetadata columnMetadata =
        SnowflakeUtil.extractColumnMetadata(rootNode, false, null);
    // then
    assertNotNull(columnMetadata);
    assertEquals(
        OBJECT_MAPPER.writeValueAsString(expectedColumnMetadata),
        OBJECT_MAPPER.writeValueAsString(columnMetadata));
  }

  private static SnowflakeColumnMetadata createExpectedMetadata(
      JsonNode rootNode, JsonNode fieldOne, JsonNode fieldTwo) throws SnowflakeSQLLoggedException {
    ColumnTypeInfo columnTypeInfo = getSnowflakeType(rootNode.path("type").asText(), null, null, 0);
    ColumnTypeInfo columnTypeInfoNodeOne =
        getSnowflakeType(fieldOne.path("type").asText(), null, null, Types.BIGINT);
    ColumnTypeInfo columnTypeInfoNodeTwo =
        getSnowflakeType(fieldTwo.path("type").asText(), null, null, Types.DECIMAL );
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
            new FieldMetadata[] {
              new FieldMetadata(
                  fieldOne.path("name").asText(),
                  fieldOne.path("type").asText(),
                  columnTypeInfoNodeOne.getColumnType(),
                  fieldOne.path("nullable").asBoolean(),
                  fieldOne.path("length").asInt(),
                  fieldOne.path("precision").asInt(),
                  fieldOne.path("scale").asInt(),
                  fieldOne.path("fixed").asBoolean(),
                  columnTypeInfoNodeOne.getSnowflakeType(),
                  null),
              new FieldMetadata(
                  fieldTwo.path("name").asText(),
                  fieldTwo.path("type").asText(),
                  columnTypeInfoNodeTwo.getColumnType(),
                  fieldTwo.path("nullable").asBoolean(),
                  fieldTwo.path("length").asInt(),
                  fieldTwo.path("precision").asInt(),
                  fieldTwo.path("scale").asInt(),
                  fieldTwo.path("fixed").asBoolean(),
                  columnTypeInfoNodeOne.getSnowflakeType(),
                  null)
            },
            rootNode.path("database").asText(),
            rootNode.path("schema").asText(),
            rootNode.path("table").asText(),
            false);
    return expectedColumnMetadata;
  }

  private static ObjectNode createRootNode() {
    ObjectNode rootNode = OBJECT_MAPPER.createObjectNode();
    String name = "STRUCT";
    rootNode.put("name", name);
    String type = "object";
    rootNode.put("type", type);
    String database = "database";
    rootNode.put("database", database);
    String schema = "schema";
    rootNode.put("schema", schema);
    String table = "table";
    rootNode.put("table", table);
    Integer precision = 2;
    rootNode.put("precision", 2);
    boolean nullable = false;
    rootNode.put("nullable", nullable);
    Integer byteLength = 128;
    rootNode.put("byteLength", byteLength);
    Integer length = 42;
    rootNode.put("length", length);
    Integer scale = 8;
    rootNode.put("scale", scale);
    return rootNode;
  }

  private JsonNode createFieldNode(
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
