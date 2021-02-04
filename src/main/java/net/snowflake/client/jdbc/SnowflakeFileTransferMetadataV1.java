/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import net.snowflake.client.jdbc.SFBaseFileTransferAgent.CommandType;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.common.core.RemoteStoreFileEncryptionMaterial;

/**
 * A class to manage metadata for upload or download files. It is introduced for distributed data
 * processing. The typical use case is: 1. The cluster master has JDBC connection to the Snowflake
 * and it can generate this object for the file transfer. 2. The cluster master node can transfer
 * the object to cluster worker. 3. The cluster worker can upload or download data with the object
 * without JDBC Connection.
 *
 * <p>NOTE: When this class is created, it only supports UPLOAD for GCS. It is created for Snowflake
 * Spark Connector.
 */
public class SnowflakeFileTransferMetadataV1
    implements SnowflakeFileTransferMetadata, Serializable {
  private static final long serialVersionUID = 1L;
  private String presignedUrl;
  private String presignedUrlFileName;
  private String encryptionMaterial_queryStageMasterKey;
  private String encryptionMaterial_queryId;
  private Long encryptionMaterial_smkId;
  private CommandType commandType;
  private StageInfo stageInfo;

  public SnowflakeFileTransferMetadataV1(
      String presignedUrl,
      String presignedUrlFileName,
      String encryptionMaterial_queryStageMasterKey,
      String encryptionMaterial_queryId,
      Long encryptionMaterial_smkId,
      CommandType commandType,
      StageInfo stageInfo) {
    this.presignedUrl = presignedUrl;
    this.presignedUrlFileName = presignedUrlFileName;
    this.encryptionMaterial_queryStageMasterKey = encryptionMaterial_queryStageMasterKey;
    this.encryptionMaterial_queryId = encryptionMaterial_queryId;
    this.encryptionMaterial_smkId = encryptionMaterial_smkId;
    this.commandType = commandType;
    this.stageInfo = stageInfo;
  }

  @Override
  public boolean isForOneFile() {
    return stageInfo.getStageType() == StageInfo.StageType.GCS;
  }

  @JsonProperty("presignedUrl")
  public String getPresignedUrl() {
    return presignedUrl;
  }

  public void setPresignedUrl(String presignedUrl) {
    this.presignedUrl = presignedUrl;
  }

  @JsonProperty("presignedUrlFileName")
  public String getPresignedUrlFileName() {
    return presignedUrlFileName;
  }

  public void setPresignedUrlFileName(String presignedUrlFileName) {
    this.presignedUrlFileName = presignedUrlFileName;
  }

  @JsonProperty("encryptionMaterial")
  public RemoteStoreFileEncryptionMaterial getEncryptionMaterial() {
    return new RemoteStoreFileEncryptionMaterial(
        encryptionMaterial_queryStageMasterKey,
        encryptionMaterial_queryId,
        encryptionMaterial_smkId);
  }

  public void setEncryptionMaterial(
      String encryptionMaterial_queryStageMasterKey,
      String encryptionMaterial_queryId,
      Long encryptionMaterial_smkId) {
    this.encryptionMaterial_queryStageMasterKey = encryptionMaterial_queryStageMasterKey;
    this.encryptionMaterial_queryId = encryptionMaterial_queryId;
    this.encryptionMaterial_smkId = encryptionMaterial_smkId;
  }

  @JsonProperty("commandType")
  public CommandType getCommandType() {
    return commandType;
  }

  public void setCommandType(CommandType commandType) {
    this.commandType = commandType;
  }

  @JsonProperty("stageInfo")
  public StageInfo getStageInfo() {
    return this.stageInfo;
  }

  public void setStageInfo(StageInfo stageInfo) {
    this.stageInfo = stageInfo;
  }
}
