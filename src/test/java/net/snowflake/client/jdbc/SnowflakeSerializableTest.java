package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeChunkDownloader.NoOpChunkDownloader;
import static net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1.ChunkFileMetadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.QueryResultFormat;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFBaseStatement;
import net.snowflake.client.core.SFStatementType;
import org.junit.jupiter.api.Assertions;
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
    Assertions.assertNotNull(s);
    Assertions.assertEquals("01b341c1-0000-772f-0000-0004189328ca", s.getQueryId());
    Assertions.assertEquals("some-db", s.getFinalDatabaseName());
    Assertions.assertEquals("some-schema", s.getFinalSchemaName());
    Assertions.assertEquals("some-warehouse", s.getFinalWarehouseName());
    Assertions.assertEquals("ENG_OPS_RL", s.getFinalRoleName());
    Assertions.assertEquals(0, s.getNumberOfBinds());
    Assertions.assertEquals(QueryResultFormat.JSON, s.getQueryResultFormat());
    Assertions.assertEquals(SFStatementType.SELECT, s.getStatementType());
    Assertions.assertEquals(
        new HashMap<String, Object>() {
          {
            put("CLIENT_PREFETCH_THREADS", 4);
            put("TIMESTAMP_OUTPUT_FORMAT", "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM");
            put("CLIENT_RESULT_CHUNK_SIZE", 128);
          }
        },
        s.getParameters());
    Assertions.assertEquals("ADCDEFGHIJdwadawYhiF81aC0wT0IU+NN8QtobPWCk=", s.getQrmk());
    Assertions.assertFalse(s.isArrayBindSupported());
    Assertions.assertEquals(1, s.getResultVersion());

    // column metadata
    Assertions.assertEquals(1, s.getColumnCount());
    Assertions.assertEquals(1, s.getResultColumnMetadata().size());
    SnowflakeColumnMetadata column = s.getResultColumnMetadata().get(0);
    Assertions.assertEquals("1", column.getName());
    Assertions.assertEquals("NUMBER", column.getTypeName());
    Assertions.assertEquals(-5, column.getType());
    Assertions.assertFalse(column.isNullable());
    Assertions.assertEquals(256, column.getLength());
    Assertions.assertEquals(0, column.getScale());
    Assertions.assertEquals(1, column.getPrecision());
    Assertions.assertEquals(SnowflakeType.FIXED, column.getBase());
    Assertions.assertEquals("some-db", column.getColumnSrcDatabase());
    Assertions.assertEquals("some-schema", column.getColumnSrcSchema());
    Assertions.assertEquals("some-table", column.getColumnSrcTable());

    // chunks metadata
    Assertions.assertEquals("[[\"1\"]]", s.getFirstChunkStringData());
    Assertions.assertEquals(1, s.getChunkHeadersMap().size());
    Assertions.assertEquals(
        "A2dDf2ff7HI8OCdsR3pK82g==",
        s.getChunkHeadersMap().get("x-amz-server-side-encryption-customer-key-md5"));
    Assertions.assertEquals(1, s.getChunkFileCount());
    Assertions.assertEquals(1, s.getChunkFileMetadatas().size());
    ChunkFileMetadata chunkMeta = s.getChunkFileMetadatas().get(0);
    Assertions.assertEquals(756, chunkMeta.getRowCount());
    Assertions.assertEquals(26828, chunkMeta.getCompressedByteSize());
    Assertions.assertEquals(312560, chunkMeta.getUncompressedByteSize());
    Assertions.assertEquals(
        "https://sfc-ds2-customer-stage.s3.us-west-2.amazonaws.com", chunkMeta.getFileURL());
    Assertions.assertNotNull(s.chunkDownloader);
    Assertions.assertTrue(expectedChunkDownloaderType.isInstance(s.chunkDownloader));
  }

  private void assertRichResultSetSerializable(SnowflakeRichResultSetSerializableV1 s) {
    // column metadata
    Assertions.assertEquals(2, s.getRichResultsColumnCount());
    Assertions.assertEquals(2, s.getRichResultsColumnMetadata().size());
    SnowflakeRichResultSetSerializableV1.SnowflakeRichResultsColumnMetadata lowerBound =
        s.getRichResultsColumnMetadata().get(0);
    Assertions.assertEquals("LOWER_BOUND", lowerBound.getName());
    Assertions.assertEquals("NUMBER", lowerBound.getTypeName());
    Assertions.assertEquals(-5, lowerBound.getType());
    Assertions.assertTrue(lowerBound.isNullable());
    Assertions.assertEquals(16777216, lowerBound.getLength());
    Assertions.assertEquals(SnowflakeType.FIXED, lowerBound.getBase());
    Assertions.assertEquals("TEMP", lowerBound.getColumnSrcDatabase());
    Assertions.assertEquals("PUBLIC", lowerBound.getColumnSrcSchema());
    Assertions.assertEquals("T_TEST", lowerBound.getColumnSrcTable());
    Assertions.assertEquals(1, lowerBound.getColumnIndex());

    SnowflakeRichResultSetSerializableV1.SnowflakeRichResultsColumnMetadata upperBound =
        s.getRichResultsColumnMetadata().get(1);
    Assertions.assertEquals("UPPER_BOUND", upperBound.getName());
    Assertions.assertEquals("NUMBER", upperBound.getTypeName());
    Assertions.assertEquals(-5, upperBound.getType());
    Assertions.assertTrue(upperBound.isNullable());
    Assertions.assertEquals(16777216, upperBound.getLength());
    Assertions.assertEquals(SnowflakeType.FIXED, upperBound.getBase());
    Assertions.assertEquals("TEMP", upperBound.getColumnSrcDatabase());
    Assertions.assertEquals("PUBLIC", upperBound.getColumnSrcSchema());
    Assertions.assertEquals("T_TEST", upperBound.getColumnSrcTable());
    Assertions.assertEquals(1, upperBound.getColumnIndex());

    // chunks metadata
    Assertions.assertEquals(
        "[[\"value1_lower\",\"value1_upper\"],[\"value2_lower\",\"value2_upper\"]]",
        s.getRichResultsFirstChunkStringData());
    Assertions.assertEquals(
        "ZXYADCDEFGHIJdwadawYhiF81aC0wT0IU+NN8QtobPWCk=", s.getRichResultsQrmk());
    Assertions.assertEquals(1, s.getRichResultsChunkHeadersMap().size());
    Assertions.assertEquals(
        "f342lkkftyf7HI8OCdsR3pK82g==",
        s.getRichResultsChunkHeadersMap().get("x-amz-server-side-encryption-customer-key-md5"));
    Assertions.assertEquals(1, s.getRichResultsChunkFileCount());
    Assertions.assertEquals(1, s.getRichResultsChunkFilesMetadata().size());
    ChunkFileMetadata chunkMeta = s.getRichResultsChunkFilesMetadata().get(0);
    Assertions.assertEquals(756, chunkMeta.getRowCount());
    Assertions.assertEquals(26828, chunkMeta.getCompressedByteSize());
    Assertions.assertEquals(312560, chunkMeta.getUncompressedByteSize());
    Assertions.assertEquals(
        "https://sfc-ds2-customer-stage.s3.us-west-2.amazonaws.com/rich-res",
        chunkMeta.getFileURL());
  }
}
