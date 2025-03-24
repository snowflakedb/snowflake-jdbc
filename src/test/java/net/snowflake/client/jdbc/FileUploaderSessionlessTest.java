package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.junit.jupiter.api.Test;

/** Tests for SnowflakeFileTransferAgent.expandFileNames. */
public class FileUploaderSessionlessTest extends FileUploaderPrep {

  private ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testGetEncryptionMaterialMissing() throws Exception {
    JsonNode modifiedNode = exampleS3JsonNode.deepCopy();
    ObjectNode foo = (ObjectNode) modifiedNode.path("data");
    foo.remove("encryptionMaterial");

    List<RemoteStoreFileEncryptionMaterial> encryptionMaterials =
        SnowflakeFileTransferAgent.getEncryptionMaterial(
            SFBaseFileTransferAgent.CommandType.UPLOAD, modifiedNode);

    assertEquals(1, encryptionMaterials.size());
    assertNull(encryptionMaterials.get(0));
  }

  @Test
  public void testGetEncryptionMaterial() throws Exception {
    List<RemoteStoreFileEncryptionMaterial> expected = new ArrayList<>();
    RemoteStoreFileEncryptionMaterial content =
        new RemoteStoreFileEncryptionMaterial(
            "EXAMPLE_QUERY_STAGE_MASTER_KEY", "EXAMPLE_QUERY_ID", 123L);
    expected.add(content);

    for (JsonNode exampleNode : exampleNodes) {
      List<RemoteStoreFileEncryptionMaterial> encryptionMaterials =
          SnowflakeFileTransferAgent.getEncryptionMaterial(
              SFBaseFileTransferAgent.CommandType.UPLOAD, exampleNode);

      assertEquals(1, encryptionMaterials.size());
      assertEquals(
          expected.get(0).getQueryStageMasterKey(),
          encryptionMaterials.get(0).getQueryStageMasterKey());
      assertEquals(expected.get(0).getQueryId(), encryptionMaterials.get(0).getQueryId());
      assertEquals(expected.get(0).getSmkId(), encryptionMaterials.get(0).getSmkId());
    }
  }

  @Test
  public void testGetS3StageData() throws Exception {
    StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleS3JsonNode, null);
    Map<String, String> expectedCreds = new HashMap<>();
    expectedCreds.put("AWS_ID", "EXAMPLE_AWS_ID");
    expectedCreds.put("AWS_KEY", "EXAMPLE_AWS_KEY");
    expectedCreds.put("AWS_KEY_ID", "EXAMPLE_AWS_KEY_ID");
    expectedCreds.put("AWS_SECRET_KEY", "EXAMPLE_AWS_SECRET_KEY");
    expectedCreds.put("AWS_TOKEN", "EXAMPLE_AWS_TOKEN");

    assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    assertEquals("stage/location/foo/", stageInfo.getLocation());
    assertEquals(expectedCreds, stageInfo.getCredentials());
    assertEquals("us-west-2", stageInfo.getRegion());
    assertEquals("null", stageInfo.getEndPoint());
    assertEquals(null, stageInfo.getStorageAccount());
    assertEquals(true, stageInfo.getIsClientSideEncrypted());
    assertEquals(true, stageInfo.getUseS3RegionalUrl());
  }

  @Test
  public void testGetS3StageDataWithStageEndpoint() throws Exception {
    StageInfo stageInfo =
        SnowflakeFileTransferAgent.getStageInfo(exampleS3StageEndpointJsonNode, null);
    Map<String, String> expectedCreds = new HashMap<>();
    expectedCreds.put("AWS_ID", "EXAMPLE_AWS_ID");
    expectedCreds.put("AWS_KEY", "EXAMPLE_AWS_KEY");
    expectedCreds.put("AWS_KEY_ID", "EXAMPLE_AWS_KEY_ID");
    expectedCreds.put("AWS_SECRET_KEY", "EXAMPLE_AWS_SECRET_KEY");
    expectedCreds.put("AWS_TOKEN", "EXAMPLE_AWS_TOKEN");

    assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    assertEquals("stage/location/foo/", stageInfo.getLocation());
    assertEquals(expectedCreds, stageInfo.getCredentials());
    assertEquals("us-west-2", stageInfo.getRegion());
    assertEquals("s3-fips.us-east-1.amazonaws.com", stageInfo.getEndPoint());
    assertEquals(null, stageInfo.getStorageAccount());
    assertEquals(true, stageInfo.getIsClientSideEncrypted());
  }

  @Test
  public void testGetAzureStageData() throws Exception {
    StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleAzureJsonNode, null);
    Map<String, String> expectedCreds = new HashMap<>();
    expectedCreds.put("AZURE_SAS_TOKEN", "EXAMPLE_AZURE_SAS_TOKEN");

    assertEquals(StageInfo.StageType.AZURE, stageInfo.getStageType());
    assertEquals("EXAMPLE_LOCATION/", stageInfo.getLocation());
    assertEquals(expectedCreds, stageInfo.getCredentials());
    assertEquals("westus", stageInfo.getRegion());
    assertEquals("blob.core.windows.net", stageInfo.getEndPoint());
    assertEquals("EXAMPLE_STORAGE_ACCOUNT", stageInfo.getStorageAccount());
    assertEquals(true, stageInfo.getIsClientSideEncrypted());
  }

  @Test
  public void testGetGCSStageData() throws Exception {
    StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleGCSJsonNode, null);
    Map<String, String> expectedCreds = new HashMap<>();

    assertEquals(StageInfo.StageType.GCS, stageInfo.getStageType());
    assertEquals("foo/tables/9224/", stageInfo.getLocation());
    assertEquals(expectedCreds, stageInfo.getCredentials());
    assertEquals("US-WEST1", stageInfo.getRegion());
    assertEquals(null, stageInfo.getEndPoint());
    assertEquals(null, stageInfo.getStorageAccount());
    assertEquals(true, stageInfo.getIsClientSideEncrypted());
  }

  @Test
  public void testGetFileTransferMetadatasS3() throws Exception {
    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleS3JsonNode);
    assertEquals(1, metadataList.size());

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1) metadataList.get(0);

    // StageInfo check
    StageInfo stageInfo = metadata.getStageInfo();

    Map<String, String> expectedCreds = new HashMap<>();
    expectedCreds.put("AWS_ID", "EXAMPLE_AWS_ID");
    expectedCreds.put("AWS_KEY", "EXAMPLE_AWS_KEY");
    expectedCreds.put("AWS_KEY_ID", "EXAMPLE_AWS_KEY_ID");
    expectedCreds.put("AWS_SECRET_KEY", "EXAMPLE_AWS_SECRET_KEY");
    expectedCreds.put("AWS_TOKEN", "EXAMPLE_AWS_TOKEN");

    assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    assertEquals("stage/location/foo/", stageInfo.getLocation());
    assertEquals(expectedCreds, stageInfo.getCredentials());
    assertEquals("us-west-2", stageInfo.getRegion());
    assertEquals("null", stageInfo.getEndPoint());
    assertEquals(null, stageInfo.getStorageAccount());
    assertEquals(true, stageInfo.getIsClientSideEncrypted());

    // EncryptionMaterial check
    assertEquals("EXAMPLE_QUERY_ID", metadata.getEncryptionMaterial().getQueryId());
    assertEquals(
        "EXAMPLE_QUERY_STAGE_MASTER_KEY",
        metadata.getEncryptionMaterial().getQueryStageMasterKey());
    assertEquals(123L, (long) metadata.getEncryptionMaterial().getSmkId());

    // Misc check
    assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, metadata.getCommandType());
    assertNull(metadata.getPresignedUrl());
    assertEquals("orders_100.csv", metadata.getPresignedUrlFileName());
  }

  @Test
  public void testGetFileTransferMetadatasS3MissingEncryption() throws Exception {
    JsonNode modifiedNode = exampleS3JsonNode.deepCopy();
    ObjectNode foo = (ObjectNode) modifiedNode.path("data");
    foo.remove("encryptionMaterial");

    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(modifiedNode);
    assertEquals(1, metadataList.size());

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1) metadataList.get(0);

    // StageInfo check
    StageInfo stageInfo = metadata.getStageInfo();

    Map<String, String> expectedCreds = new HashMap<>();
    expectedCreds.put("AWS_ID", "EXAMPLE_AWS_ID");
    expectedCreds.put("AWS_KEY", "EXAMPLE_AWS_KEY");
    expectedCreds.put("AWS_KEY_ID", "EXAMPLE_AWS_KEY_ID");
    expectedCreds.put("AWS_SECRET_KEY", "EXAMPLE_AWS_SECRET_KEY");
    expectedCreds.put("AWS_TOKEN", "EXAMPLE_AWS_TOKEN");

    assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    assertEquals("stage/location/foo/", stageInfo.getLocation());
    assertEquals(expectedCreds, stageInfo.getCredentials());
    assertEquals("us-west-2", stageInfo.getRegion());
    assertEquals("null", stageInfo.getEndPoint());
    assertEquals(null, stageInfo.getStorageAccount());
    assertEquals(true, stageInfo.getIsClientSideEncrypted());

    // EncryptionMaterial check
    assertNull(metadata.getEncryptionMaterial().getQueryId());
    assertNull(metadata.getEncryptionMaterial().getQueryStageMasterKey());
    assertNull(metadata.getEncryptionMaterial().getSmkId());

    // Misc check
    assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, metadata.getCommandType());
    assertNull(metadata.getPresignedUrl());
    assertEquals("orders_100.csv", metadata.getPresignedUrlFileName());
  }

  @Test
  public void testGetFileTransferMetadatasAzure() throws Exception {
    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleAzureJsonNode);
    assertEquals(1, metadataList.size());

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1) metadataList.get(0);

    // StageInfo check
    StageInfo stageInfo = metadata.getStageInfo();

    Map<String, String> expectedCreds = new HashMap<>();
    expectedCreds.put("AZURE_SAS_TOKEN", "EXAMPLE_AZURE_SAS_TOKEN");

    assertEquals(StageInfo.StageType.AZURE, stageInfo.getStageType());
    assertEquals("EXAMPLE_LOCATION/", stageInfo.getLocation());
    assertEquals(expectedCreds, stageInfo.getCredentials());
    assertEquals("westus", stageInfo.getRegion());
    assertEquals("blob.core.windows.net", stageInfo.getEndPoint());
    assertEquals("EXAMPLE_STORAGE_ACCOUNT", stageInfo.getStorageAccount());
    assertEquals(true, stageInfo.getIsClientSideEncrypted());

    // EncryptionMaterial check
    assertEquals("EXAMPLE_QUERY_ID", metadata.getEncryptionMaterial().getQueryId());
    assertEquals(
        "EXAMPLE_QUERY_STAGE_MASTER_KEY",
        metadata.getEncryptionMaterial().getQueryStageMasterKey());
    assertEquals(123L, (long) metadata.getEncryptionMaterial().getSmkId());

    // Misc check
    assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, metadata.getCommandType());
    assertNull(metadata.getPresignedUrl());
    assertEquals("orders_100.csv", metadata.getPresignedUrlFileName());
  }

  @Test
  public void testGetFileTransferMetadatasGCS() throws Exception {
    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleGCSJsonNode);
    assertEquals(1, metadataList.size());

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1) metadataList.get(0);

    // StageInfo check
    StageInfo stageInfo = metadata.getStageInfo();

    Map<String, String> expectedCreds = new HashMap<>();

    assertEquals(StageInfo.StageType.GCS, stageInfo.getStageType());
    assertEquals("foo/tables/9224/", stageInfo.getLocation());
    assertEquals(expectedCreds, stageInfo.getCredentials());
    assertEquals("US-WEST1", stageInfo.getRegion());
    assertEquals(null, stageInfo.getEndPoint());
    assertEquals(null, stageInfo.getStorageAccount());
    assertEquals(true, stageInfo.getIsClientSideEncrypted());
    assertEquals(Optional.empty(), stageInfo.gcsCustomEndpoint());

    // EncryptionMaterial check
    assertEquals("EXAMPLE_QUERY_ID", metadata.getEncryptionMaterial().getQueryId());
    assertEquals(
        "EXAMPLE_QUERY_STAGE_MASTER_KEY",
        metadata.getEncryptionMaterial().getQueryStageMasterKey());
    assertEquals(123L, (long) metadata.getEncryptionMaterial().getSmkId());

    // Misc check
    assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, metadata.getCommandType());
    assertEquals("EXAMPLE_PRESIGNED_URL", metadata.getPresignedUrl());
    assertEquals("orders_100.csv", metadata.getPresignedUrlFileName());
  }

  @Test
  public void testGetFileTransferMetadataGCSWithUseRegionalUrl() throws Exception {
    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleGCSJsonNodeWithUseRegionalUrl);
    assertEquals(1, metadataList.size());

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1) metadataList.get(0);

    StageInfo stageInfo = metadata.getStageInfo();

    assertTrue(stageInfo.getUseRegionalUrl());
    assertEquals(Optional.of("storage.us-west1.rep.googleapis.com"), stageInfo.gcsCustomEndpoint());
  }

  @Test
  public void testGetFileTransferMetadataGCSWithEndPoint() throws Exception {
    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleGCSJsonNodeWithEndPoint);
    assertEquals(1, metadataList.size());

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1) metadataList.get(0);

    StageInfo stageInfo = metadata.getStageInfo();

    assertFalse(stageInfo.getUseRegionalUrl());
    assertEquals(Optional.of("example.com"), stageInfo.gcsCustomEndpoint());
  }

  @Test
  public void testGetFileTransferMetadatasUploadError() throws Exception {
    JsonNode downloadNode = mapper.readTree("{\"data\": {\"command\": \"DOWNLOAD\"}}");
    SnowflakeSQLException err =
        assertThrows(
            SnowflakeSQLException.class,
            () -> SnowflakeFileTransferAgent.getFileTransferMetadatas(downloadNode));
    assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
    assertEquals(
        "JDBC driver internal error: This API only supports PUT commands.", err.getMessage());
  }

  @Test
  public void testGetFileTransferMetadatasEncryptionMaterialError() throws Exception {
    JsonNode garbageNode = mapper.readTree("{\"data\": {\"src_locations\": [1, 2]}}");
    SnowflakeSQLException err =
        assertThrows(
            SnowflakeSQLException.class,
            () -> SnowflakeFileTransferAgent.getFileTransferMetadatas(garbageNode));
    assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
    assertTrue(
        err.getMessage().contains("JDBC driver internal error: Failed to parse the credentials"));
  }

  @Test
  public void testGetFileTransferMetadatasUnsupportedLocationError() {
    JsonNode modifiedNode = exampleS3JsonNode.deepCopy();
    ObjectNode foo = (ObjectNode) modifiedNode.path("data").path("stageInfo");
    foo.put("locationType", "LOCAL_FS");
    SnowflakeSQLException err =
        assertThrows(
            SnowflakeSQLException.class,
            () -> SnowflakeFileTransferAgent.getFileTransferMetadatas(modifiedNode));
    assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
    assertTrue(err.getMessage().contains("JDBC driver internal error: This API only supports"));
  }

  @Test
  public void testGetFileTransferMetadatasSrcLocationsArrayError() throws JsonProcessingException {
    JsonNode garbageNode = mapper.readTree("{\"data\": {\"src_locations\": \"abc\"}}");
    SnowflakeSQLException err =
        assertThrows(
            SnowflakeSQLException.class,
            () -> SnowflakeFileTransferAgent.getFileTransferMetadatas(garbageNode));
    assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
    assertTrue(
        err.getMessage().contains("JDBC driver internal error: src_locations must be an array"));
  }

  @Test
  public void testGetFileMetadatasEncryptionMaterialsException() {
    JsonNode modifiedNode = exampleS3JsonNode.deepCopy();
    ObjectNode foo = (ObjectNode) modifiedNode.path("data");
    foo.put("encryptionMaterial", "[1, 2, 3]]");
    SnowflakeSQLException err =
        assertThrows(
            SnowflakeSQLException.class,
            () -> SnowflakeFileTransferAgent.getFileTransferMetadatas(modifiedNode));
    assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
    assertTrue(err.getMessage().contains("Failed to parse encryptionMaterial"));
  }
}
