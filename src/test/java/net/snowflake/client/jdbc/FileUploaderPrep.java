package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;

/** File uploader test prep reused by IT/connection tests and sessionless tests */
abstract class FileUploaderPrep extends BaseJDBCTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  static JsonNode exampleS3JsonNode;
  static JsonNode exampleS3StageEndpointJsonNode;
  static JsonNode exampleAzureJsonNode;
  static JsonNode exampleGCSJsonNode;
  static JsonNode exampleGCSJsonNodeWithUseRegionalUrl;
  static JsonNode exampleGCSJsonNodeWithEndPoint;
  static List<JsonNode> exampleNodes;

  private static JsonNode readJsonFromFile(String name) throws IOException {
    try (InputStream is =
        FileUploaderPrep.class.getResourceAsStream("/FileUploaderPrep/" + name + ".json")) {
      return mapper.readTree(is);
    }
  }

  @BeforeAll
  public static void setup() throws Exception {
    exampleS3JsonNode = readJsonFromFile("exampleS3");
    exampleS3StageEndpointJsonNode = readJsonFromFile("exampleS3WithStageEndpoint");
    exampleAzureJsonNode = readJsonFromFile("exampleAzure");
    exampleGCSJsonNode = readJsonFromFile("exampleGCS");
    exampleGCSJsonNodeWithUseRegionalUrl = readJsonFromFile("exampleGCSWithUseRegionalUrl");
    exampleGCSJsonNodeWithEndPoint = readJsonFromFile("exampleGCSWithEndpoint");
    exampleNodes =
        Arrays.asList(
            exampleS3JsonNode,
            exampleAzureJsonNode,
            exampleGCSJsonNode,
            exampleGCSJsonNodeWithUseRegionalUrl,
            exampleGCSJsonNodeWithEndPoint);
  }
}
