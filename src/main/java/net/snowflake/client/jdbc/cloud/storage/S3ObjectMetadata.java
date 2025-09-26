package net.snowflake.client.jdbc.cloud.storage;

import java.util.Map;
import net.snowflake.client.jdbc.SnowflakeUtil;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * s3 implementation of platform independent StorageObjectMetadata interface, wraps an S3
 * ObjectMetadata class
 *
 * <p>It only supports a limited set of metadata properties currently used by the JDBC client
 */
public class S3ObjectMetadata implements StorageObjectMetadata {
  private HeadObjectResponse objectMetadata;

  S3ObjectMetadata() {
    objectMetadata = HeadObjectResponse.builder().build();
  }
  // Construct from an AWS S3 ObjectMetadata object
  S3ObjectMetadata(HeadObjectResponse meta) {
    objectMetadata = meta;
  }

  @Override
  public Map<String, String> getUserMetadata() {
    return SnowflakeUtil.createCaseInsensitiveMap();
  }

  @Override
  public long getContentLength() {
    return;
  }

  @Override
  public void setContentLength(long contentLength) {}

  @Override
  public void addUserMetadata(String key, String value) {
    /*AWS SDK for Java v2 migration: Transform for ObjectMetadata setter - addUserMetadata - is not supported, please manually migrate the code by setting it on the v2 request/response object.*/ objectMetadata
        .addUserMetadata(key, value);
  }

  @Override
  public void setContentEncoding(String encoding) {}

  @Override
  public String getContentEncoding() {
    return;
  }
  /**
   * @return Returns the encapsulated AWS S3 metadata object
   */
  HeadObjectResponse getS3ObjectMetadata() {
    return objectMetadata;
  }
}
