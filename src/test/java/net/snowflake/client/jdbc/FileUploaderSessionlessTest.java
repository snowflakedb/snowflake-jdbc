/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.junit.Assert;
import org.junit.Test;

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

    Assert.assertEquals(1, encryptionMaterials.size());
    Assert.assertNull(encryptionMaterials.get(0));
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

      Assert.assertEquals(1, encryptionMaterials.size());
      Assert.assertEquals(
          expected.get(0).getQueryStageMasterKey(),
          encryptionMaterials.get(0).getQueryStageMasterKey());
      Assert.assertEquals(expected.get(0).getQueryId(), encryptionMaterials.get(0).getQueryId());
      Assert.assertEquals(expected.get(0).getSmkId(), encryptionMaterials.get(0).getSmkId());
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

    Assert.assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    Assert.assertEquals("stage/location/foo/", stageInfo.getLocation());
    Assert.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assert.assertEquals("us-west-2", stageInfo.getRegion());
    Assert.assertEquals("null", stageInfo.getEndPoint());
    Assert.assertEquals(null, stageInfo.getStorageAccount());
    Assert.assertEquals(true, stageInfo.getIsClientSideEncrypted());
    Assert.assertEquals(true, stageInfo.getUseS3RegionalUrl());
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

    Assert.assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    Assert.assertEquals("stage/location/foo/", stageInfo.getLocation());
    Assert.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assert.assertEquals("us-west-2", stageInfo.getRegion());
    Assert.assertEquals("s3-fips.us-east-1.amazonaws.com", stageInfo.getEndPoint());
    Assert.assertEquals(null, stageInfo.getStorageAccount());
    Assert.assertEquals(true, stageInfo.getIsClientSideEncrypted());
  }

  @Test
  public void testGetAzureStageData() throws Exception {
    StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleAzureJsonNode, null);
    Map<String, String> expectedCreds = new HashMap<>();
    expectedCreds.put("AZURE_SAS_TOKEN", "EXAMPLE_AZURE_SAS_TOKEN");

    Assert.assertEquals(StageInfo.StageType.AZURE, stageInfo.getStageType());
    Assert.assertEquals("EXAMPLE_LOCATION/", stageInfo.getLocation());
    Assert.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assert.assertEquals("westus", stageInfo.getRegion());
    Assert.assertEquals("blob.core.windows.net", stageInfo.getEndPoint());
    Assert.assertEquals("EXAMPLE_STORAGE_ACCOUNT", stageInfo.getStorageAccount());
    Assert.assertEquals(true, stageInfo.getIsClientSideEncrypted());
  }

  @Test
  public void testGetGCSStageData() throws Exception {
    StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleGCSJsonNode, null);
    Map<String, String> expectedCreds = new HashMap<>();

    Assert.assertEquals(StageInfo.StageType.GCS, stageInfo.getStageType());
    Assert.assertEquals("foo/tables/9224/", stageInfo.getLocation());
    Assert.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assert.assertEquals("US-WEST1", stageInfo.getRegion());
    Assert.assertEquals(null, stageInfo.getEndPoint());
    Assert.assertEquals(null, stageInfo.getStorageAccount());
    Assert.assertEquals(true, stageInfo.getIsClientSideEncrypted());
  }

  @Test
  public void testGetFileTransferMetadatasS3() throws Exception {
    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleS3JsonNode);
    Assert.assertEquals(1, metadataList.size());

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

    Assert.assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    Assert.assertEquals("stage/location/foo/", stageInfo.getLocation());
    Assert.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assert.assertEquals("us-west-2", stageInfo.getRegion());
    Assert.assertEquals("null", stageInfo.getEndPoint());
    Assert.assertEquals(null, stageInfo.getStorageAccount());
    Assert.assertEquals(true, stageInfo.getIsClientSideEncrypted());

    // EncryptionMaterial check
    Assert.assertEquals("EXAMPLE_QUERY_ID", metadata.getEncryptionMaterial().getQueryId());
    Assert.assertEquals(
        "EXAMPLE_QUERY_STAGE_MASTER_KEY",
        metadata.getEncryptionMaterial().getQueryStageMasterKey());
    Assert.assertEquals(123L, (long) metadata.getEncryptionMaterial().getSmkId());

    // Misc check
    Assert.assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, metadata.getCommandType());
    Assert.assertNull(metadata.getPresignedUrl());
    Assert.assertEquals("orders_100.csv", metadata.getPresignedUrlFileName());
  }

  @Test
  public void testGetFileTransferMetadatasS3MissingEncryption() throws Exception {
    JsonNode modifiedNode = exampleS3JsonNode.deepCopy();
    ObjectNode foo = (ObjectNode) modifiedNode.path("data");
    foo.remove("encryptionMaterial");

    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(modifiedNode);
    Assert.assertEquals(1, metadataList.size());

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

    Assert.assertEquals(StageInfo.StageType.S3, stageInfo.getStageType());
    Assert.assertEquals("stage/location/foo/", stageInfo.getLocation());
    Assert.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assert.assertEquals("us-west-2", stageInfo.getRegion());
    Assert.assertEquals("null", stageInfo.getEndPoint());
    Assert.assertEquals(null, stageInfo.getStorageAccount());
    Assert.assertEquals(true, stageInfo.getIsClientSideEncrypted());

    // EncryptionMaterial check
    Assert.assertNull(metadata.getEncryptionMaterial().getQueryId());
    Assert.assertNull(metadata.getEncryptionMaterial().getQueryStageMasterKey());
    Assert.assertNull(metadata.getEncryptionMaterial().getSmkId());

    // Misc check
    Assert.assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, metadata.getCommandType());
    Assert.assertNull(metadata.getPresignedUrl());
    Assert.assertEquals("orders_100.csv", metadata.getPresignedUrlFileName());
  }

  @Test
  public void testGetFileTransferMetadatasAzure() throws Exception {
    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleAzureJsonNode);
    Assert.assertEquals(1, metadataList.size());

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1) metadataList.get(0);

    // StageInfo check
    StageInfo stageInfo = metadata.getStageInfo();

    Map<String, String> expectedCreds = new HashMap<>();
    expectedCreds.put("AZURE_SAS_TOKEN", "EXAMPLE_AZURE_SAS_TOKEN");

    Assert.assertEquals(StageInfo.StageType.AZURE, stageInfo.getStageType());
    Assert.assertEquals("EXAMPLE_LOCATION/", stageInfo.getLocation());
    Assert.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assert.assertEquals("westus", stageInfo.getRegion());
    Assert.assertEquals("blob.core.windows.net", stageInfo.getEndPoint());
    Assert.assertEquals("EXAMPLE_STORAGE_ACCOUNT", stageInfo.getStorageAccount());
    Assert.assertEquals(true, stageInfo.getIsClientSideEncrypted());

    // EncryptionMaterial check
    Assert.assertEquals("EXAMPLE_QUERY_ID", metadata.getEncryptionMaterial().getQueryId());
    Assert.assertEquals(
        "EXAMPLE_QUERY_STAGE_MASTER_KEY",
        metadata.getEncryptionMaterial().getQueryStageMasterKey());
    Assert.assertEquals(123L, (long) metadata.getEncryptionMaterial().getSmkId());

    // Misc check
    Assert.assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, metadata.getCommandType());
    Assert.assertNull(metadata.getPresignedUrl());
    Assert.assertEquals("orders_100.csv", metadata.getPresignedUrlFileName());
  }

  @Test
  public void testGetFileTransferMetadatasGCS() throws Exception {
    List<SnowflakeFileTransferMetadata> metadataList =
        SnowflakeFileTransferAgent.getFileTransferMetadatas(exampleGCSJsonNode);
    Assert.assertEquals(1, metadataList.size());

    SnowflakeFileTransferMetadataV1 metadata =
        (SnowflakeFileTransferMetadataV1) metadataList.get(0);

    // StageInfo check
    StageInfo stageInfo = metadata.getStageInfo();

    Map<String, String> expectedCreds = new HashMap<>();

    Assert.assertEquals(StageInfo.StageType.GCS, stageInfo.getStageType());
    Assert.assertEquals("foo/tables/9224/", stageInfo.getLocation());
    Assert.assertEquals(expectedCreds, stageInfo.getCredentials());
    Assert.assertEquals("US-WEST1", stageInfo.getRegion());
    Assert.assertEquals(null, stageInfo.getEndPoint());
    Assert.assertEquals(null, stageInfo.getStorageAccount());
    Assert.assertEquals(true, stageInfo.getIsClientSideEncrypted());

    // EncryptionMaterial check
    Assert.assertEquals("EXAMPLE_QUERY_ID", metadata.getEncryptionMaterial().getQueryId());
    Assert.assertEquals(
        "EXAMPLE_QUERY_STAGE_MASTER_KEY",
        metadata.getEncryptionMaterial().getQueryStageMasterKey());
    Assert.assertEquals(123L, (long) metadata.getEncryptionMaterial().getSmkId());

    // Misc check
    Assert.assertEquals(SFBaseFileTransferAgent.CommandType.UPLOAD, metadata.getCommandType());
    Assert.assertEquals("EXAMPLE_PRESIGNED_URL", metadata.getPresignedUrl());
    Assert.assertEquals("orders_100.csv", metadata.getPresignedUrlFileName());
  }

  @Test
  public void testGetFileTransferMetadatasUploadError() throws Exception {
    JsonNode downloadNode = mapper.readTree("{\"data\": {\"command\": \"DOWNLOAD\"}}");
    try {
      SnowflakeFileTransferAgent.getFileTransferMetadatas(downloadNode);
      Assert.assertTrue(false);
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assert.assertEquals(
          "JDBC driver internal error: This API only supports PUT commands.", err.getMessage());
    }
  }

  @Test
  public void testGetFileTransferMetadatasEncryptionMaterialError() throws Exception {
    JsonNode garbageNode = mapper.readTree("{\"data\": {\"src_locations\": [1, 2]}}");
    try {
      SnowflakeFileTransferAgent.getFileTransferMetadatas(garbageNode);
      Assert.assertTrue(false);
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assert.assertTrue(
          err.getMessage().contains("JDBC driver internal error: Failed to parse the credentials"));
    }
  }

  @Test
  public void testGetFileTransferMetadatasUnsupportedLocationError() throws Exception {
    JsonNode modifiedNode = exampleS3JsonNode.deepCopy();
    ObjectNode foo = (ObjectNode) modifiedNode.path("data").path("stageInfo");
    foo.put("locationType", "LOCAL_FS");
    try {
      SnowflakeFileTransferAgent.getFileTransferMetadatas(modifiedNode);
      Assert.assertTrue(false);
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assert.assertTrue(
          err.getMessage().contains("JDBC driver internal error: This API only supports"));
    }
  }

  @Test
  public void testGetFileTransferMetadatasSrcLocationsArrayError() throws JsonProcessingException {
    JsonNode garbageNode = mapper.readTree("{\"data\": {\"src_locations\": \"abc\"}}");
    try {
      SnowflakeFileTransferAgent.getFileTransferMetadatas(garbageNode);
      Assert.assertTrue(false);
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assert.assertTrue(
          err.getMessage().contains("JDBC driver internal error: src_locations must be an array"));
    }
  }

  @Test
  public void testGetFileMetadatasEncryptionMaterialsException() {
    JsonNode modifiedNode = exampleS3JsonNode.deepCopy();
    ObjectNode foo = (ObjectNode) modifiedNode.path("data");
    foo.put("encryptionMaterial", "[1, 2, 3]]");
    try {
      SnowflakeFileTransferAgent.getFileTransferMetadatas(modifiedNode);
      Assert.assertTrue(false);
    } catch (SnowflakeSQLException err) {
      Assert.assertEquals((long) ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getErrorCode());
      Assert.assertTrue(err.getMessage().contains("Failed to parse encryptionMaterial"));
    }
  }
}
