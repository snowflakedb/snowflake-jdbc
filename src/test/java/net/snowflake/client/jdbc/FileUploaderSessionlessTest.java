/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for SnowflakeFileTransferAgent.expandFileNames. */
public class FileUploaderSessionlessTest extends FileUploaderPrepIT {

  private ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testGetEncryptionMaterialMissing() throws Exception {
    JsonNode modifiedNode = exampleS3JsonNode.deepCopy();
    ObjectNode foo = (ObjectNode) modifiedNode.path("data");
    foo.remove("encryptionMaterial");

    List<RemoteStoreFileEncryptionMaterial> encryptionMaterials =
        SnowflakeFileTransferAgent.getEncryptionMaterial(
            SFBaseFileTransferAgent.CommandType.UPLOAD, modifiedNode);

    Assertions.assertEquals(1, encryptionMaterials.size());
    Assertions.assertNull(encryptionMaterials.get(0));
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

      Assertions.assertEquals(1, encryptionMaterials.size());
      Assertions.assertEquals(expected.get(0).getQueryStageMasterKey(), encryptionMaterials.get(0).getQueryStageMasterKey());
      Assertions.assertEquals(expected.get(0).getQueryId(), encryptionMaterials.get(0).getQueryId());
      Assertions.assertEquals(expected.get(0).getSmkId(), encryptionMaterials.get(0).getSmkId());
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

    Assertions.assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    Assertions.assertEquals("stage/location/foo/", stageInfo.getLocation());
    Assertions.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assertions.assertEquals("us-west-2", stageInfo.getRegion());
    Assertions.assertEquals("null", stageInfo.getEndPoint());
    Assertions.assertEquals(null, stageInfo.getStorageAccount());
    Assertions.assertEquals(true, stageInfo.getIsClientSideEncrypted());
    Assertions.assertEquals(true, stageInfo.getUseS3RegionalUrl());
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

    Assertions.assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    Assertions.assertEquals("stage/location/foo/", stageInfo.getLocation());
    Assertions.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assertions.assertEquals("us-west-2", stageInfo.getRegion());
    Assertions.assertEquals("s3-fips.us-east-1.amazonaws.com", stageInfo.getEndPoint());
    Assertions.assertEquals(null, stageInfo.getStorageAccount());
    Assertions.assertEquals(true, stageInfo.getIsClientSideEncrypted());
  }

  @Test
  public void testGetAzureStageData() throws Exception {
    StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleAzureJsonNode, null);
    Map<String, String> expectedCreds = new HashMap<>();
    expectedCreds.put("AZURE_SAS_TOKEN", "EXAMPLE_AZURE_SAS_TOKEN");

    Assertions.assertEquals(StageInfo.StageType.AZURE, stageInfo.getStageType());
    Assertions.assertEquals("EXAMPLE_LOCATION/", stageInfo.getLocation());
    Assertions.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assertions.assertEquals("westus", stageInfo.getRegion());
    Assertions.assertEquals("blob.core.windows.net", stageInfo.getEndPoint());
    Assertions.assertEquals("EXAMPLE_STORAGE_ACCOUNT", stageInfo.getStorageAccount());
    Assertions.assertEquals(true, stageInfo.getIsClientSideEncrypted());
  }

  @Test
  public void testGetGCSStageData() throws Exception {
    StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleGCSJsonNode, null);
    Map<String, String> expectedCreds = new HashMap<>();

    Assertions.assertEquals(StageInfo.StageType.GCS, stageInfo.getStageType());
    Assertions.assertEquals("foo/tables/9224/", stageInfo.getLocation());
    Assertions.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assertions.assertEquals("US-WEST1", stageInfo.getRegion());
    Assertions.assertEquals(null, stageInfo.getEndPoint());
    Assertions.assertEquals(null, stageInfo.getStorageAccount());
    Assertions.assertEquals(true, stageInfo.getIsClientSideEncrypted());
  }

  @Test
  public void testGetFileTransferMetadatasS3() throws Exception {
    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleS3JsonNode);
    Assertions.assertEquals(1, metadataList.size());

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

    Assertions.assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    Assertions.assertEquals("stage/location/foo/", stageInfo.getLocation());
    Assertions.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assertions.assertEquals("us-west-2", stageInfo.getRegion());
    Assertions.assertEquals("null", stageInfo.getEndPoint());
    Assertions.assertEquals(null, stageInfo.getStorageAccount());
    Assertions.assertEquals(true, stageInfo.getIsClientSideEncrypted());

    // EncryptionMaterial check
    Assertions.assertEquals("EXAMPLE_QUERY_ID", metadata.getEncryptionMaterial().getQueryId());
    Assertions.assertEquals("EXAMPLE_QUERY_STAGE_MASTER_KEY", metadata.getEncryptionMaterial().getQueryStageMasterKey());
    Assertions.assertEquals(123L, (long) metadata.getEncryptionMaterial().getSmkId());

    // Misc check
    Assertions.assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, metadata.getCommandType());
    Assertions.assertNull(metadata.getPresignedUrl());
    Assertions.assertEquals("orders_100.csv", metadata.getPresignedUrlFileName());
  }

  @Test
  public void testGetFileTransferMetadatasS3MissingEncryption() throws Exception {
    JsonNode modifiedNode = exampleS3JsonNode.deepCopy();
    ObjectNode foo = (ObjectNode) modifiedNode.path("data");
    foo.remove("encryptionMaterial");

    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(modifiedNode);
    Assertions.assertEquals(1, metadataList.size());

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

    Assertions.assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    Assertions.assertEquals("stage/location/foo/", stageInfo.getLocation());
    Assertions.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assertions.assertEquals("us-west-2", stageInfo.getRegion());
    Assertions.assertEquals("null", stageInfo.getEndPoint());
    Assertions.assertEquals(null, stageInfo.getStorageAccount());
    Assertions.assertEquals(true, stageInfo.getIsClientSideEncrypted());

    // EncryptionMaterial check
    Assertions.assertNull(metadata.getEncryptionMaterial().getQueryId());
    Assertions.assertNull(metadata.getEncryptionMaterial().getQueryStageMasterKey());
    Assertions.assertNull(metadata.getEncryptionMaterial().getSmkId());

    // Misc check
    Assertions.assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, metadata.getCommandType());
    Assertions.assertNull(metadata.getPresignedUrl());
    Assertions.assertEquals("orders_100.csv", metadata.getPresignedUrlFileName());
  }

  @Test
  public void testGetFileTransferMetadatasAzure() throws Exception {
    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleAzureJsonNode);
    Assertions.assertEquals(1, metadataList.size());

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1) metadataList.get(0);

    // StageInfo check
    StageInfo stageInfo = metadata.getStageInfo();

    Map<String, String> expectedCreds = new HashMap<>();
    expectedCreds.put("AZURE_SAS_TOKEN", "EXAMPLE_AZURE_SAS_TOKEN");

    Assertions.assertEquals(StageInfo.StageType.AZURE, stageInfo.getStageType());
    Assertions.assertEquals("EXAMPLE_LOCATION/", stageInfo.getLocation());
    Assertions.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assertions.assertEquals("westus", stageInfo.getRegion());
    Assertions.assertEquals("blob.core.windows.net", stageInfo.getEndPoint());
    Assertions.assertEquals("EXAMPLE_STORAGE_ACCOUNT", stageInfo.getStorageAccount());
    Assertions.assertEquals(true, stageInfo.getIsClientSideEncrypted());

    // EncryptionMaterial check
    Assertions.assertEquals("EXAMPLE_QUERY_ID", metadata.getEncryptionMaterial().getQueryId());
    Assertions.assertEquals("EXAMPLE_QUERY_STAGE_MASTER_KEY", metadata.getEncryptionMaterial().getQueryStageMasterKey());
    Assertions.assertEquals(123L, (long) metadata.getEncryptionMaterial().getSmkId());

    // Misc check
    Assertions.assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, metadata.getCommandType());
    Assertions.assertNull(metadata.getPresignedUrl());
    Assertions.assertEquals("orders_100.csv", metadata.getPresignedUrlFileName());
  }

  @Test
  public void testGetFileTransferMetadatasGCS() throws Exception {
    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleGCSJsonNode);
    Assertions.assertEquals(1, metadataList.size());

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1) metadataList.get(0);

    // StageInfo check
    StageInfo stageInfo = metadata.getStageInfo();

    Map<String, String> expectedCreds = new HashMap<>();

    Assertions.assertEquals(StageInfo.StageType.GCS, stageInfo.getStageType());
    Assertions.assertEquals("foo/tables/9224/", stageInfo.getLocation());
    Assertions.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assertions.assertEquals("US-WEST1", stageInfo.getRegion());
    Assertions.assertEquals(null, stageInfo.getEndPoint());
    Assertions.assertEquals(null, stageInfo.getStorageAccount());
    Assertions.assertEquals(true, stageInfo.getIsClientSideEncrypted());

    // EncryptionMaterial check
    Assertions.assertEquals("EXAMPLE_QUERY_ID", metadata.getEncryptionMaterial().getQueryId());
    Assertions.assertEquals("EXAMPLE_QUERY_STAGE_MASTER_KEY", metadata.getEncryptionMaterial().getQueryStageMasterKey());
    Assertions.assertEquals(123L, (long) metadata.getEncryptionMaterial().getSmkId());

    // Misc check
    Assertions.assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, metadata.getCommandType());
    Assertions.assertEquals("EXAMPLE_PRESIGNED_URL", metadata.getPresignedUrl());
    Assertions.assertEquals("orders_100.csv", metadata.getPresignedUrlFileName());
  }

  @Test
  public void testGetFileTransferMetadatasUploadError() throws Exception {
    JsonNode downloadNode = mapper.readTree("{\"data\": {\"command\": \"DOWNLOAD\"}}");
    try {
      SnowflakeFileTransferAgent.getFileTransferMetadatas(downloadNode);
      Assertions.assertTrue(false);
    } catch (SnowflakeSQLException err) {
      Assertions.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assertions.assertEquals("JDBC driver internal error: This API only supports PUT commands.", err.getMessage());
    }
  }

  @Test
  public void testGetFileTransferMetadatasEncryptionMaterialError() throws Exception {
    JsonNode garbageNode = mapper.readTree("{\"data\": {\"src_locations\": [1, 2]}}");
    try {
      SnowflakeFileTransferAgent.getFileTransferMetadatas(garbageNode);
      Assertions.assertTrue(false);
    } catch (SnowflakeSQLException err) {
      Assertions.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assertions.assertTrue(err.getMessage().contains("JDBC driver internal error: Failed to parse the credentials"));
    }
  }

  @Test
  public void testGetFileTransferMetadatasUnsupportedLocationError() throws Exception {
    JsonNode modifiedNode = exampleS3JsonNode.deepCopy();
    ObjectNode foo = (ObjectNode) modifiedNode.path("data").path("stageInfo");
    foo.put("locationType", "LOCAL_FS");
    try {
      SnowflakeFileTransferAgent.getFileTransferMetadatas(modifiedNode);
      Assertions.assertTrue(false);
    } catch (SnowflakeSQLException err) {
      Assertions.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assertions.assertTrue(err.getMessage().contains("JDBC driver internal error: This API only supports"));
    }
  }

  @Test
  public void testGetFileTransferMetadatasSrcLocationsArrayError() throws JsonProcessingException {
    JsonNode garbageNode = mapper.readTree("{\"data\": {\"src_locations\": \"abc\"}}");
    try {
      SnowflakeFileTransferAgent.getFileTransferMetadatas(garbageNode);
      Assertions.assertTrue(false);
    } catch (SnowflakeSQLException err) {
      Assertions.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assertions.assertTrue(err.getMessage().contains("JDBC driver internal error: src_locations must be an array"));
    }
  }

  @Test
  public void testGetFileMetadatasEncryptionMaterialsException() {
    JsonNode modifiedNode = exampleS3JsonNode.deepCopy();
    ObjectNode foo = (ObjectNode) modifiedNode.path("data");
    foo.put("encryptionMaterial", "[1, 2, 3]]");
    try {
      SnowflakeFileTransferAgent.getFileTransferMetadatas(modifiedNode);
      Assertions.assertTrue(false);
    } catch (SnowflakeSQLException err) {
      Assertions.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assertions.assertTrue(err.getMessage().contains("Failed to parse encryptionMaterial"));
    }
  }
}
