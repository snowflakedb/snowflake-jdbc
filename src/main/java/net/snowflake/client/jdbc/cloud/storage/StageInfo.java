package net.snowflake.client.jdbc.cloud.storage;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.core.Event;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/** Encapsulates all the required stage properties used by GET/PUT for Azure and S3 stages */
public class StageInfo implements Serializable {
  private static final SFLogger logger = SFLoggerFactory.getLogger(Event.class);

  public enum StageType {
    S3,
    AZURE,
    LOCAL_FS,
    GCS
  }

  // First value represents key encryption mode, second value represents file content encryption
  // mode.
  public enum Ciphers {
    AESECB_AESCBC,
    AESGCM_AESGCM
  }

  private static final long serialVersionUID = 1L;
  private StageType stageType; // The stage type
  private String location; // The container or bucket
  private Map<?, ?> credentials; // the credentials required for the  stage
  private String region; // AWS/S3/GCS region (S3/GCS only)
  private String endPoint; // The Azure Storage endpoint (Azure only)
  private String storageAccount; // The Azure Storage account (Azure only)
  private String presignedUrl; // GCS gives us back a presigned URL instead of a cred
  private boolean isClientSideEncrypted; // whether to encrypt/decrypt files on the stage
  private Ciphers ciphers;
  private boolean useS3RegionalUrl; // whether to use s3 regional URL (AWS Only)
  private Properties proxyProperties;

  /*
   * @deprecated Use {@link #createStageInfo(String, String, Map, String, String, String, boolean, String)}
   */
  @Deprecated
  public static StageInfo createStageInfo(
      String locationType,
      String location,
      Map<?, ?> credentials,
      String region,
      String endPoint,
      String storageAccount,
      boolean isClientSideEncrypted)
      throws IllegalArgumentException {
    return createStageInfo(
        locationType,
        location,
        credentials,
        region,
        endPoint,
        storageAccount,
        isClientSideEncrypted,
        null);
  }

  /*
   * Creates a StageInfo object
   * Validates that the necessary Stage info arguments are specified
   *
   * @param locationType the type of stage, i.e. AZURE/S3
   * @param location The container/bucket
   * @param credentials Map of cloud provider credentials
   * @param region The geographic region where the stage is located (S3 only)
   * @param endPoint The Azure Storage end point (Azure only)
   * @param storageAccount The Azure Storage account (azure only)
   * @param isClientSideEncrypted Whether the stage should use client-side encryption
   * @throws IllegalArgumentException one or more parameters required were missing
   */
  @SnowflakeJdbcInternalApi
  public static StageInfo createStageInfo(
      String locationType,
      String location,
      Map<?, ?> credentials,
      String region,
      String endPoint,
      String storageAccount,
      boolean isClientSideEncrypted,
      String ciphersString) {
    StageType stageType;
    // Ensure that all the required parameters are specified
    switch (locationType) {
      case "AZURE":
        stageType = StageType.AZURE;
        if (!isSpecified(location)
            || !isSpecified(endPoint)
            || !isSpecified(storageAccount)
            || credentials == null) {
          throw new IllegalArgumentException("Incomplete parameters specified for Azure stage");
        }
        break;

      case "S3":
        stageType = StageType.S3;
        if (!isSpecified(location) || !isSpecified(region) || credentials == null) {
          throw new IllegalArgumentException("Incomplete parameters specified for S3 stage");
        }
        break;

      case "GCS":
        stageType = StageType.GCS;
        if (!isSpecified(location) || credentials == null) {
          throw new IllegalArgumentException("Incomplete parameters specified for GCS stage");
        }
        break;

      case "LOCAL_FS":
        stageType = StageType.LOCAL_FS;
        if (!isSpecified(location)) {
          throw new IllegalArgumentException("Incomplete parameters specific for local stage");
        }
        break;

      default:
        throw new IllegalArgumentException("Invalid stage type: " + locationType);
    }
    Ciphers ciphers = null;
    if (isClientSideEncrypted) {
      if (ciphersString == null || ciphersString.isEmpty()) {
        logger.debug("No ciphers specified for stage, using ECB/CBC mode");
        ciphers = Ciphers.AESECB_AESCBC;
      } else if (ciphersString.equals("AES_CBC")) {
        ciphers = Ciphers.AESECB_AESCBC;
      } else if (ciphersString.equals("AES_GCM") || ciphersString.equals("AES_GCM,AES_CBC")) {
        ciphers = Ciphers.AESGCM_AESGCM;
      } else {
        throw new IllegalArgumentException("Invalid ciphers: " + ciphersString);
      }
    }
    return new StageInfo(
        stageType,
        location,
        credentials,
        region,
        endPoint,
        storageAccount,
        isClientSideEncrypted,
        ciphers);
  }

  /*
   * StageInfo constructor, accessible only via the createStageInfo method
   * Assumes valid parameters are specified
   *clear
   *
   * @param stageType the type of stage, i.e. AZURE/S3
   * @param location The container/bucket
   * @param credentials Map of cloud provider credentials
   * @param region The geographic region where the stage is located (S3 only)
   * @param endPoint The Azure Storage end point (Azure only)
   * @param storageAccount The Azure Storage account (azure only)
   * @param isClientSideEncrypted Whether the stage uses client-side encryption
   */
  private StageInfo(
      StageType stageType,
      String location,
      Map<?, ?> credentials,
      String region,
      String endPoint,
      String storageAccount,
      boolean isClientSideEncrypted,
      Ciphers ciphers) {
    this.stageType = stageType;
    this.location = location;
    this.credentials = credentials;
    this.region = region;
    this.endPoint = endPoint;
    this.storageAccount = storageAccount;
    this.isClientSideEncrypted = isClientSideEncrypted;
    this.ciphers = ciphers;
  }

  public StageType getStageType() {
    return stageType;
  }

  public String getLocation() {
    return location;
  }

  public Map<?, ?> getCredentials() {
    return credentials;
  }

  public void setCredentials(Map<?, ?> credentials) {
    this.credentials = credentials;
  }

  public String getRegion() {
    return region;
  }

  public String getEndPoint() {
    return endPoint;
  }

  public String getStorageAccount() {
    return storageAccount;
  }

  public String getPresignedUrl() {
    return presignedUrl;
  }

  public void setPresignedUrl(String presignedUrl) {
    this.presignedUrl = presignedUrl;
  }

  public boolean getIsClientSideEncrypted() {
    return isClientSideEncrypted;
  }

  public Ciphers getCiphers() {
    return ciphers;
  }

  public void setUseS3RegionalUrl(boolean useS3RegionalUrl) {
    this.useS3RegionalUrl = useS3RegionalUrl;
  }

  public boolean getUseS3RegionalUrl() {
    return useS3RegionalUrl;
  }

  private static boolean isSpecified(String arg) {
    return !(arg == null || arg.equalsIgnoreCase(""));
  }

  public void setProxyProperties(Properties proxyProperties) {
    this.proxyProperties = proxyProperties;
  }
  ;

  public Properties getProxyProperties() {
    return proxyProperties;
  }
}
