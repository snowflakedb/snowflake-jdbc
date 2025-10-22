package net.snowflake.client.internal.jdbc.cloud.storage;

import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * s3 implementation of platform independent StorageObjectMetadata interface
 *
 * <p>It only supports a limited set of metadata properties currently used by the JDBC client
 */
public class S3ObjectMetadata implements StorageObjectMetadata {
  private Map<String, String> userMetadata = new HashMap<>();
  private Long contentLength;
  private String contentEncoding;

  S3ObjectMetadata() {}

  public S3ObjectMetadata(HeadObjectResponse meta) {
    userMetadata = meta.metadata();
    contentLength = meta.contentLength();
    contentEncoding = meta.contentEncoding();
  }

  public S3ObjectMetadata(PutObjectRequest meta) {
    userMetadata = meta.metadata();
    contentLength = meta.contentLength();
    contentEncoding = meta.contentEncoding();
  }

  @Override
  public Map<String, String> getUserMetadata() {
    return SnowflakeUtil.createCaseInsensitiveMap(userMetadata);
  }

  public Map<String, String> setUserMetadata(Map<String, String> metadata) {
    return this.userMetadata = metadata;
  }

  @Override
  public long getContentLength() {
    return this.contentLength;
  }

  @Override
  public void setContentLength(long contentLength) {
    this.contentLength = contentLength;
  }

  @Override
  public void addUserMetadata(String key, String value) {
    userMetadata.put(key, value);
  }

  @Override
  public void setContentEncoding(String encoding) {
    this.contentEncoding = encoding;
  }

  @Override
  public String getContentEncoding() {
    return contentEncoding;
  }

  /**
   * @return Returns the encapsulated AWS S3 metadata request
   */
  PutObjectRequest getS3PutObjectRequest() {
    return PutObjectRequest.builder()
        .metadata(userMetadata)
        .contentLength(contentLength)
        .contentEncoding(contentEncoding)
        .build();
  }
}
