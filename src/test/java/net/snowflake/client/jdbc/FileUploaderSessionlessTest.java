/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for SnowflakeFileTransferAgent.expandFileNames */
public class FileUploaderSessionlessTest {
  @Rule public TemporaryFolder folder = new TemporaryFolder();
  private ObjectMapper mapper = new ObjectMapper();

  private final String exampleS3JsonString =
      "{\n"
          + "  \"data\": {\n"
          + "    \"uploadInfo\": {\n"
          + "      \"locationType\": \"S3\",\n"
          + "      \"location\": \"example/location\",\n"
          + "      \"path\": \"tables/19805757505/\",\n"
          + "      \"region\": \"us-west-2\",\n"
          + "      \"storageAccount\": null,\n"
          + "      \"isClientSideEncrypted\": true,\n"
          + "      \"creds\": {\n"
          + "        \"AWS_KEY_ID\": \"EXAMPLE_AWS_KEY_ID\",\n"
          + "        \"AWS_SECRET_KEY\": \"EXAMPLE_AWS_SECRET_KEY\",\n"
          + "        \"AWS_TOKEN\": \"EXAMPLE_AWS_TOKEN\",\n"
          + "        \"AWS_ID\": \"EXAMPLE_AWS_ID\",\n"
          + "        \"AWS_KEY\": \"EXAMPLE_AWS_KEY\"\n"
          + "      },\n"
          + "      \"presignedUrl\": null,\n"
          + "      \"endPoint\": null\n"
          + "    },\n"
          + "    \"src_locations\": [\n"
          + "      \"/tmp/files/orders_100.csv\"\n"
          + "    ],\n"
          + "    \"parallel\": 4,\n"
          + "    \"threshold\": 209715200,\n"
          + "    \"autoCompress\": true,\n"
          + "    \"overwrite\": false,\n"
          + "    \"sourceCompression\": \"auto_detect\",\n"
          + "    \"clientShowEncryptionParameter\": true,\n"
          + "    \"queryId\": \"EXAMPLE_QUERY_ID\",\n"
          + "    \"encryptionMaterial\": {\n"
          + "      \"queryStageMasterKey\": \"EXAMPLE_QUERY_STAGE_MASTER_KEY\",\n"
          + "      \"queryId\": \"EXAMPLE_QUERY_ID\",\n"
          + "      \"smkId\": 123\n"
          + "    },\n"
          + "    \"stageInfo\": {\n"
          + "      \"locationType\": \"S3\",\n"
          + "      \"location\": \"stage/location/foo/\",\n"
          + "      \"path\": \"tables/19805757505/\",\n"
          + "      \"region\": \"us-west-2\",\n"
          + "      \"storageAccount\": null,\n"
          + "      \"isClientSideEncrypted\": true,\n"
          + "      \"creds\": {\n"
          + "        \"AWS_KEY_ID\": \"EXAMPLE_AWS_KEY_ID\",\n"
          + "        \"AWS_SECRET_KEY\": \"EXAMPLE_AWS_SECRET_KEY\",\n"
          + "        \"AWS_TOKEN\": \"EXAMPLE_AWS_TOKEN\",\n"
          + "        \"AWS_ID\": \"EXAMPLE_AWS_ID\",\n"
          + "        \"AWS_KEY\": \"EXAMPLE_AWS_KEY\"\n"
          + "      },\n"
          + "      \"presignedUrl\": null,\n"
          + "      \"endPoint\": null\n"
          + "    },\n"
          + "    \"command\": \"UPLOAD\",\n"
          + "    \"kind\": null,\n"
          + "    \"operation\": \"Node\"\n"
          + "  },\n"
          + "  \"code\": null,\n"
          + "  \"message\": null,\n"
          + "  \"success\": true\n"
          + "}";

  private final String exampleAzureJsonString =
      "{\n"
          + "  \"data\": {\n"
          + "    \"uploadInfo\": {\n"
          + "      \"locationType\": \"AZURE\",\n"
          + "      \"location\": \"EXAMPLE_LOCATION/\",\n"
          + "      \"path\": \"EXAMPLE_PATH/\",\n"
          + "      \"region\": \"westus\",\n"
          + "      \"storageAccount\": \"sfcdevstage\",\n"
          + "      \"isClientSideEncrypted\": true,\n"
          + "      \"creds\": {\n"
          + "        \"AZURE_SAS_TOKEN\": \"EXAMPLE_AZURE_SAS_TOKEN\"\n"
          + "      },\n"
          + "      \"presignedUrl\": null,\n"
          + "      \"endPoint\": \"blob.core.windows.net\"\n"
          + "    },\n"
          + "    \"src_locations\": [\n"
          + "      \"/foo/orders_100.csv\"\n"
          + "    ],\n"
          + "    \"parallel\": 4,\n"
          + "    \"threshold\": 209715200,\n"
          + "    \"autoCompress\": true,\n"
          + "    \"overwrite\": false,\n"
          + "    \"sourceCompression\": \"auto_detect\",\n"
          + "    \"clientShowEncryptionParameter\": false,\n"
          + "    \"queryId\": \"EXAMPLE_QUERY_ID\",\n"
          + "    \"encryptionMaterial\": {\n"
          + "      \"queryStageMasterKey\": \"EXAMPLE_QUERY_STAGE_MASTER_KEY\",\n"
          + "      \"queryId\": \"EXAMPLE_QUERY_ID\",\n"
          + "      \"smkId\": 123\n"
          + "    },\n"
          + "    \"stageInfo\": {\n"
          + "      \"locationType\": \"AZURE\",\n"
          + "      \"location\": \"EXAMPLE_LOCATION/\",\n"
          + "      \"path\": \"EXAMPLE_PATH/\",\n"
          + "      \"region\": \"westus\",\n"
          + "      \"storageAccount\": \"EXAMPLE_STORAGE_ACCOUNT\",\n"
          + "      \"isClientSideEncrypted\": true,\n"
          + "      \"creds\": {\n"
          + "        \"AZURE_SAS_TOKEN\": \"EXAMPLE_AZURE_SAS_TOKEN\"\n"
          + "      },\n"
          + "      \"presignedUrl\": null,\n"
          + "      \"endPoint\": \"blob.core.windows.net\"\n"
          + "    },\n"
          + "    \"command\": \"UPLOAD\",\n"
          + "    \"kind\": null,\n"
          + "    \"operation\": \"Node\"\n"
          + "  },\n"
          + "  \"code\": null,\n"
          + "  \"message\": null,\n"
          + "  \"success\": true\n"
          + "}";

  private final String exampleGCSJsonString =
      "{\n"
          + "  \"data\": {\n"
          + "    \"uploadInfo\": {\n"
          + "      \"locationType\": \"GCS\",\n"
          + "      \"location\": \"foo/tables/9224/\",\n"
          + "      \"path\": \"tables/9224/\",\n"
          + "      \"region\": \"US-WEST1\",\n"
          + "      \"storageAccount\": \"\",\n"
          + "      \"isClientSideEncrypted\": true,\n"
          + "      \"creds\": {},\n"
          + "      \"presignedUrl\": \"EXAMPLE_PRESIGNED_URL\",\n"
          + "      \"endPoint\": \"\"\n"
          + "    },\n"
          + "    \"src_locations\": [\n"
          + "      \"/foo/bart/orders_100.csv\"\n"
          + "    ],\n"
          + "    \"parallel\": 4,\n"
          + "    \"threshold\": 209715200,\n"
          + "    \"autoCompress\": true,\n"
          + "    \"overwrite\": false,\n"
          + "    \"sourceCompression\": \"auto_detect\",\n"
          + "    \"clientShowEncryptionParameter\": false,\n"
          + "    \"queryId\": \"EXAMPLE_QUERY_ID\",\n"
          + "    \"encryptionMaterial\": {\n"
          + "      \"queryStageMasterKey\": \"EXAMPLE_QUERY_STAGE_MASTER_KEY\",\n"
          + "      \"queryId\": \"EXAMPLE_QUERY_ID\",\n"
          + "      \"smkId\": 123\n"
          + "    },\n"
          + "    \"stageInfo\": {\n"
          + "      \"locationType\": \"GCS\",\n"
          + "      \"location\": \"foo/tables/9224/\",\n"
          + "      \"path\": \"tables/9224/\",\n"
          + "      \"region\": \"US-WEST1\",\n"
          + "      \"storageAccount\": \"\",\n"
          + "      \"isClientSideEncrypted\": true,\n"
          + "      \"creds\": {},\n"
          + "      \"presignedUrl\": \"EXAMPLE_PRESIGNED_URL\",\n"
          + "      \"endPoint\": \"\"\n"
          + "    },\n"
          + "    \"command\": \"UPLOAD\",\n"
          + "    \"kind\": null,\n"
          + "    \"operation\": \"Node\"\n"
          + "  },\n"
          + "  \"code\": null,\n"
          + "  \"message\": null,\n"
          + "  \"success\": true\n"
          + "}";

  protected JsonNode exampleS3JsonNode;
  protected JsonNode exampleAzureJsonNode;
  private JsonNode exampleGCSJsonNode;
  private List<JsonNode> exampleNodes;

  @Before
  public void setup() throws Exception {
    exampleS3JsonNode = mapper.readTree(exampleS3JsonString);
    exampleAzureJsonNode = mapper.readTree(exampleAzureJsonString);
    exampleGCSJsonNode = mapper.readTree(exampleGCSJsonString);
    exampleNodes = Arrays.asList(exampleS3JsonNode, exampleAzureJsonNode, exampleGCSJsonNode);
  }

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
    StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleS3JsonNode);
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
    Assert.assertEquals(null, stageInfo.getEndPoint());
    Assert.assertEquals(null, stageInfo.getStorageAccount());
    Assert.assertEquals(true, stageInfo.getIsClientSideEncrypted());
  }

  @Test
  public void testGetAzureStageData() throws Exception {
    StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleAzureJsonNode);
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
    StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleGCSJsonNode);
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
    Assert.assertEquals(null, stageInfo.getEndPoint());
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
}
