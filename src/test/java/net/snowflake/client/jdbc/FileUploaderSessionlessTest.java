/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

  private final String exampleJsonString =
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

  private JsonNode exampleJsonNode;

  @Before
  public void setup() throws Exception {
    exampleJsonNode = mapper.readTree(exampleJsonString);
  }

  @Test
  public void testGetEncryptionMaterial() throws Exception {
    List<RemoteStoreFileEncryptionMaterial> encryptionMaterials =
        SnowflakeFileTransferAgent.getEncryptionMaterial(
            SFBaseFileTransferAgent.CommandType.UPLOAD, exampleJsonNode);

    List<RemoteStoreFileEncryptionMaterial> expected = new ArrayList<>();
    RemoteStoreFileEncryptionMaterial content =
        new RemoteStoreFileEncryptionMaterial(
            "EXAMPLE_QUERY_STAGE_MASTER_KEY", "EXAMPLE_QUERY_ID", 123L);
    expected.add(content);
    Assert.assertEquals(1, encryptionMaterials.size());
    Assert.assertEquals(
        expected.get(0).getQueryStageMasterKey(),
        encryptionMaterials.get(0).getQueryStageMasterKey());
    Assert.assertEquals(expected.get(0).getQueryId(), encryptionMaterials.get(0).getQueryId());
    Assert.assertEquals(expected.get(0).getSmkId(), encryptionMaterials.get(0).getSmkId());
  }

  @Test
  public void testGetStageData() throws Exception {
    StageInfo stageInfo = SnowflakeFileTransferAgent.getStageInfo(exampleJsonNode);
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
}
