package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeChunkDownloader.NoOpChunkDownloader;
import static net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1.ChunkFileMetadata;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.QueryResultFormat;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFBaseStatement;
import net.snowflake.client.core.SFStatementType;
import org.junit.jupiter.api.Test;

public class SnowflakeSerializableTest {

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();
  private static final String STANDARD_SERIALIZABLE_V1_JSON_STRING =
      "{\n"
          + "  \"data\": {\n"
          + "    \"parameters\": [\n"
          + "      {\n"
          + "        \"name\": \"CLIENT_PREFETCH_THREADS\",\n"
          + "        \"value\": 4\n"
          + "      },\n"
          + "      {\n"
          + "        \"name\": \"TIMESTAMP_OUTPUT_FORMAT\",\n"
          + "        \"value\": \"YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM\"\n"
          + "      },\n"
          + "      {\n"
          + "        \"name\": \"CLIENT_RESULT_CHUNK_SIZE\",\n"
          + "        \"value\": 128\n"
          + "      }\n"
          + "    ],\n"
          + "    \"rowtype\": [\n"
          + "      {\n"
          + "        \"name\": \"1\",\n"
          + "        \"database\": \"some-db\",\n"
          + "        \"schema\": \"some-schema\",\n"
          + "        \"table\": \"some-table\",\n"
          + "        \"byteLength\": null,\n"
          + "        \"type\": \"fixed\",\n"
          + "        \"length\": 256,\n"
          + "        \"scale\": 0,\n"
          + "        \"precision\": 1,\n"
          + "        \"nullable\": false,\n"
          + "        \"collation\": null\n"
          + "      }\n"
          + "    ],\n"
          + "    \"rowset\": [\n"
          + "      [\n"
          + "        \"1\"\n"
          + "      ]\n"
          + "    ],\n"
          + "    \"qrmk\": \"ADCDEFGHIJdwadawYhiF81aC0wT0IU+NN8QtobPWCk=\",\n"
          + "    \"chunkHeaders\": {\n"
          + "      \"x-amz-server-side-encryption-customer-key-md5\": \"A2dDf2ff7HI8OCdsR3pK82g==\"\n"
          + "    },\n"
          + "    \"chunks\": [\n"
          + "      {\n"
          + "        \"url\": \"https://sfc-ds2-customer-stage.s3.us-west-2.amazonaws.com\",\n"
          + "        \"rowCount\": 756,\n"
          + "        \"uncompressedSize\": 312560,\n"
          + "        \"compressedSize\": 26828\n"
          + "      }\n"
          + "    ],\n"
          + "    \"total\": 1,\n"
          + "    \"returned\": 1,\n"
          + "    \"queryId\": \"01b341c1-0000-772f-0000-0004189328ca\",\n"
          + "    \"databaseProvider\": \"some-db-provider\",\n"
          + "    \"finalDatabaseName\": \"some-db\",\n"
          + "    \"finalSchemaName\": \"some-schema\",\n"
          + "    \"finalWarehouseName\": \"some-warehouse\",\n"
          + "    \"finalRoleName\": \"ENG_OPS_RL\",\n"
          + "    \"numberOfBinds\": 0,\n"
          + "    \"arrayBindSupported\": false,\n"
          + "    \"statementTypeId\": 4096,\n"
          + "    \"version\": 1,\n"
          + "    \"sendResultTime\": 1711499620154,\n"
          + "    \"queryResultFormat\": \"json\",\n"
          + "    \"queryContext\": {\n"
          + "      \"entries\": [\n"
          + "        {\n"
          + "          \"id\": 0,\n"
          + "          \"timestamp\": 55456940208204,\n"
          + "          \"priority\": 0\n"
          + "        }\n"
          + "      ]\n"
          + "    }\n"
          + "  },\n"
          + "  \"code\": null,\n"
          + "  \"message\": null,\n"
          + "  \"success\": true\n"
          + "}";

  public static final String RICH_RESULTS_SERIALIZABLE_V1_JSON_STRING =
      "{\n"
          + "  \"data\": {\n"
          + "    \"parameters\": [\n"
          + "      {\n"
          + "        \"name\": \"CLIENT_PREFETCH_THREADS\",\n"
          + "        \"value\": 4\n"
          + "      },\n"
          + "      {\n"
          + "        \"name\": \"TIMESTAMP_OUTPUT_FORMAT\",\n"
          + "        \"value\": \"YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM\"\n"
          + "      },\n"
          + "      {\n"
          + "        \"name\": \"CLIENT_RESULT_CHUNK_SIZE\",\n"
          + "        \"value\": 128\n"
          + "      }\n"
          + "    ],\n"
          + "    \"rowtype\": [\n"
          + "      {\n"
          + "        \"name\": \"1\",\n"
          + "        \"database\": \"some-db\",\n"
          + "        \"schema\": \"some-schema\",\n"
          + "        \"table\": \"some-table\",\n"
          + "        \"byteLength\": null,\n"
          + "        \"type\": \"fixed\",\n"
          + "        \"length\": 256,\n"
          + "        \"scale\": 0,\n"
          + "        \"precision\": 1,\n"
          + "        \"nullable\": false,\n"
          + "        \"collation\": null\n"
          + "      }\n"
          + "    ],\n"
          + "    \"rowset\": [\n"
          + "      [\n"
          + "        \"1\"\n"
          + "      ]\n"
          + "    ],\n"
          + "    \"qrmk\": \"ADCDEFGHIJdwadawYhiF81aC0wT0IU+NN8QtobPWCk=\",\n"
          + "    \"chunkHeaders\": {\n"
          + "      \"x-amz-server-side-encryption-customer-key-md5\": \"A2dDf2ff7HI8OCdsR3pK82g==\"\n"
          + "    },\n"
          + "    \"chunks\": [\n"
          + "      {\n"
          + "        \"url\": \"https://sfc-ds2-customer-stage.s3.us-west-2.amazonaws.com\",\n"
          + "        \"rowCount\": 756,\n"
          + "        \"uncompressedSize\": 312560,\n"
          + "        \"compressedSize\": 26828\n"
          + "      }\n"
          + "    ],\n"
          + "    \"total\": 1,\n"
          + "    \"returned\": 1,\n"
          + "    \"queryId\": \"01b341c1-0000-772f-0000-0004189328ca\",\n"
          + "    \"databaseProvider\": \"some-db-provider\",\n"
          + "    \"finalDatabaseName\": \"some-db\",\n"
          + "    \"finalSchemaName\": \"some-schema\",\n"
          + "    \"finalWarehouseName\": \"some-warehouse\",\n"
          + "    \"finalRoleName\": \"ENG_OPS_RL\",\n"
          + "    \"numberOfBinds\": 0,\n"
          + "    \"arrayBindSupported\": false,\n"
          + "    \"statementTypeId\": 4096,\n"
          + "    \"version\": 1,\n"
          + "    \"sendResultTime\": 1711499620154,\n"
          + "    \"queryResultFormat\": \"json\",\n"
          + "    \"queryContext\": {\n"
          + "      \"entries\": [\n"
          + "        {\n"
          + "          \"id\": 0,\n"
          + "          \"timestamp\": 55456940208204,\n"
          + "          \"priority\": 0\n"
          + "        }\n"
          + "      ]\n"
          + "    }\n"
          + "  },\n"
          + "  \"richResult\": {\n"
          + "    \"rowtype\": [\n"
          + "        {\n"
          + "          \"name\": \"LOWER_BOUND\",\n"
          + "          \"database\": \"TEMP\",\n"
          + "          \"schema\": \"PUBLIC\",\n"
          + "          \"table\": \"T_TEST\",\n"
          + "          \"precision\": null,\n"
          + "          \"byteLength\": 16777216,\n"
          + "          \"type\": \"fixed\",\n"
          + "          \"scale\": null,\n"
          + "          \"nullable\": true,\n"
          + "          \"collation\": null,\n"
          + "          \"length\": 16777216,\n"
          + "          \"columnIndexing\": 1\n"
          + "      },\n"
          + "      {\n"
          + "          \"name\": \"UPPER_BOUND\",\n"
          + "          \"database\": \"TEMP\",\n"
          + "          \"schema\": \"PUBLIC\",\n"
          + "          \"table\": \"T_TEST\",\n"
          + "          \"precision\": null,\n"
          + "          \"byteLength\": 16777216,\n"
          + "          \"type\": \"fixed\",\n"
          + "          \"scale\": null,\n"
          + "          \"nullable\": true,\n"
          + "          \"collation\": null,\n"
          + "          \"length\": 16777216,\n"
          + "          \"columnIndexing\": 1\n"
          + "      }\n"
          + "    ],\n"
          + "    \"rowset\": [\n"
          + "      [\n"
          + "        \"value1_lower\",\n"
          + "        \"value1_upper\"\n"
          + "      ],\n"
          + "      [\n"
          + "        \"value2_lower\",\n"
          + "        \"value2_upper\"\n"
          + "      ]\n"
          + "\n"
          + "    ],\n"
          + "    \"qrmk\": \"ZXYADCDEFGHIJdwadawYhiF81aC0wT0IU+NN8QtobPWCk=\",\n"
          + "    \"chunkHeaders\": {\n"
          + "      \"x-amz-server-side-encryption-customer-key-md5\": \"f342lkkftyf7HI8OCdsR3pK82g==\"\n"
          + "    },\n"
          + "    \"chunks\": [\n"
          + "      {\n"
          + "        \"url\": \"https://sfc-ds2-customer-stage.s3.us-west-2.amazonaws.com/rich-res\",\n"
          + "        \"rowCount\": 756,\n"
          + "        \"uncompressedSize\": 312560,\n"
          + "        \"compressedSize\": 26828\n"
          + "      }\n"
          + "    ],\n"
          + "    \"total\": 1,\n"
          + "    \"queryId\": \"01b341c1-0000-772f-0000-0004189328ca\",\n"
          + "    \"queryResultFormat\": \"json\"\n"
          + "  },\n"
          + "  \"code\": null,\n"
          + "  \"message\": null,\n"
          + "  \"success\": true\n"
          + "}";

  private static final SFBaseSession MOCK_SESSION =
      new MockConnectionTest.MockSnowflakeConnectionImpl().getSFSession();
  private static final SFBaseStatement MOCK_STATEMENT =
      new MockConnectionTest.MockSnowflakeConnectionImpl().getSFStatement();

  @Test
  public void shouldProperlyCreateSerializableV1()
      throws JsonProcessingException, SnowflakeSQLException {
    JsonNode rootNode = OBJECT_MAPPER.readTree(STANDARD_SERIALIZABLE_V1_JSON_STRING);
    SnowflakeResultSetSerializableV1 s =
        SnowflakeResultSetSerializableV1.create(
            rootNode, MOCK_SESSION, MOCK_STATEMENT, new DefaultResultStreamProvider());
    assertRegularResultSetSerializable(s, SnowflakeChunkDownloader.class);
  }

  @Test
  public void shouldCreateSerializableWithNoOpChunksDownloader()
      throws JsonProcessingException, SnowflakeSQLException {
    JsonNode rootNode = OBJECT_MAPPER.readTree(STANDARD_SERIALIZABLE_V1_JSON_STRING);
    SnowflakeResultSetSerializableV1 s =
        SnowflakeResultSetSerializableV1.createWithChunksPrefetchDisabled(
            rootNode, MOCK_SESSION, MOCK_STATEMENT);
    assertRegularResultSetSerializable(s, NoOpChunkDownloader.class);
  }

  @Test
  public void shouldProperlyCreateRichSerializableV1()
      throws JsonProcessingException, SnowflakeSQLException {
    JsonNode rootNode = OBJECT_MAPPER.readTree(RICH_RESULTS_SERIALIZABLE_V1_JSON_STRING);
    SnowflakeRichResultSetSerializableV1 s =
        SnowflakeRichResultSetSerializableV1.createWithChunksPrefetchDisabled(
            rootNode, MOCK_SESSION, MOCK_STATEMENT);
    assertRegularResultSetSerializable(s, NoOpChunkDownloader.class);
    assertRichResultSetSerializable(s);
  }

  private void assertRegularResultSetSerializable(
      SnowflakeResultSetSerializableV1 s, Class<?> expectedChunkDownloaderType) {
    assertNotNull(s);
    assertEquals("01b341c1-0000-772f-0000-0004189328ca", s.getQueryId());
    assertEquals("some-db", s.getFinalDatabaseName());
    assertEquals("some-schema", s.getFinalSchemaName());
    assertEquals("some-warehouse", s.getFinalWarehouseName());
    assertEquals("ENG_OPS_RL", s.getFinalRoleName());
    assertEquals(0, s.getNumberOfBinds());
    assertEquals(QueryResultFormat.JSON, s.getQueryResultFormat());
    assertEquals(SFStatementType.SELECT, s.getStatementType());
    assertEquals(
        new HashMap<String, Object>() {
          {
            put("CLIENT_PREFETCH_THREADS", 4);
            put("TIMESTAMP_OUTPUT_FORMAT", "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM");
            put("CLIENT_RESULT_CHUNK_SIZE", 128);
          }
        },
        s.getParameters());
    assertEquals("ADCDEFGHIJdwadawYhiF81aC0wT0IU+NN8QtobPWCk=", s.getQrmk());
    assertFalse(s.isArrayBindSupported());
    assertEquals(1, s.getResultVersion());

    // column metadata
    assertEquals(1, s.getColumnCount());
    assertEquals(1, s.getResultColumnMetadata().size());
    SnowflakeColumnMetadata column = s.getResultColumnMetadata().get(0);
    assertEquals("1", column.getName());
    assertEquals("NUMBER", column.getTypeName());
    assertEquals(-5, column.getType());
    assertFalse(column.isNullable());
    assertEquals(256, column.getLength());
    assertEquals(0, column.getScale());
    assertEquals(1, column.getPrecision());
    assertEquals(SnowflakeType.FIXED, column.getBase());
    assertEquals("some-db", column.getColumnSrcDatabase());
    assertEquals("some-schema", column.getColumnSrcSchema());
    assertEquals("some-table", column.getColumnSrcTable());

    // chunks metadata
    assertEquals("[[\"1\"]]", s.getFirstChunkStringData());
    assertEquals(1, s.getChunkHeadersMap().size());
    assertEquals(
        "A2dDf2ff7HI8OCdsR3pK82g==",
        s.getChunkHeadersMap().get("x-amz-server-side-encryption-customer-key-md5"));
    assertEquals(1, s.getChunkFileCount());
    assertEquals(1, s.getChunkFileMetadatas().size());
    ChunkFileMetadata chunkMeta = s.getChunkFileMetadatas().get(0);
    assertEquals(756, chunkMeta.getRowCount());
    assertEquals(26828, chunkMeta.getCompressedByteSize());
    assertEquals(312560, chunkMeta.getUncompressedByteSize());
    assertEquals(
        "https://sfc-ds2-customer-stage.s3.us-west-2.amazonaws.com", chunkMeta.getFileURL());
    assertNotNull(s.chunkDownloader);
    assertTrue(expectedChunkDownloaderType.isInstance(s.chunkDownloader));
  }

  private void assertRichResultSetSerializable(SnowflakeRichResultSetSerializableV1 s) {
    // column metadata
    assertEquals(2, s.getRichResultsColumnCount());
    assertEquals(2, s.getRichResultsColumnMetadata().size());
    SnowflakeRichResultSetSerializableV1.SnowflakeRichResultsColumnMetadata lowerBound =
        s.getRichResultsColumnMetadata().get(0);
    assertEquals("LOWER_BOUND", lowerBound.getName());
    assertEquals("NUMBER", lowerBound.getTypeName());
    assertEquals(-5, lowerBound.getType());
    assertTrue(lowerBound.isNullable());
    assertEquals(16777216, lowerBound.getLength());
    assertEquals(SnowflakeType.FIXED, lowerBound.getBase());
    assertEquals("TEMP", lowerBound.getColumnSrcDatabase());
    assertEquals("PUBLIC", lowerBound.getColumnSrcSchema());
    assertEquals("T_TEST", lowerBound.getColumnSrcTable());
    assertEquals(1, lowerBound.getColumnIndex());

    SnowflakeRichResultSetSerializableV1.SnowflakeRichResultsColumnMetadata upperBound =
        s.getRichResultsColumnMetadata().get(1);
    assertEquals("UPPER_BOUND", upperBound.getName());
    assertEquals("NUMBER", upperBound.getTypeName());
    assertEquals(-5, upperBound.getType());
    assertTrue(upperBound.isNullable());
    assertEquals(16777216, upperBound.getLength());
    assertEquals(SnowflakeType.FIXED, upperBound.getBase());
    assertEquals("TEMP", upperBound.getColumnSrcDatabase());
    assertEquals("PUBLIC", upperBound.getColumnSrcSchema());
    assertEquals("T_TEST", upperBound.getColumnSrcTable());
    assertEquals(1, upperBound.getColumnIndex());

    // chunks metadata
    assertEquals(
        "[[\"value1_lower\",\"value1_upper\"],[\"value2_lower\",\"value2_upper\"]]",
        s.getRichResultsFirstChunkStringData());
    assertEquals("ZXYADCDEFGHIJdwadawYhiF81aC0wT0IU+NN8QtobPWCk=", s.getRichResultsQrmk());
    assertEquals(1, s.getRichResultsChunkHeadersMap().size());
    assertEquals(
        "f342lkkftyf7HI8OCdsR3pK82g==",
        s.getRichResultsChunkHeadersMap().get("x-amz-server-side-encryption-customer-key-md5"));
    assertEquals(1, s.getRichResultsChunkFileCount());
    assertEquals(1, s.getRichResultsChunkFilesMetadata().size());
    ChunkFileMetadata chunkMeta = s.getRichResultsChunkFilesMetadata().get(0);
    assertEquals(756, chunkMeta.getRowCount());
    assertEquals(26828, chunkMeta.getCompressedByteSize());
    assertEquals(312560, chunkMeta.getUncompressedByteSize());
    assertEquals(
        "https://sfc-ds2-customer-stage.s3.us-west-2.amazonaws.com/rich-res",
        chunkMeta.getFileURL());
  }
}
