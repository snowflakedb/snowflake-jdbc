package net.snowflake.client.jdbc.cloud.storage;

import com.amazonaws.services.s3.model.ObjectMetadata;
import java.util.Map;
import net.snowflake.client.jdbc.SnowflakeUtil;

/** Implementation of StorageObjectMetadata for S3 for remote storage object metadata. */
public class S3StorageObjectMetadata implements StorageObjectMetadata {
  private ObjectMetadata s3Metadata;

  public S3StorageObjectMetadata(ObjectMetadata s3Metadata) {
    if (s3Metadata == null) {
      throw new IllegalArgumentException("s3Metadata must not be null");
    }
    this.s3Metadata = s3Metadata;
  }

  /**
   * @return returns a Map/key-value pairs of metadata properties
   */
  @Override
  public Map<String, String> getUserMetadata() {
    return SnowflakeUtil.createCaseInsensitiveMap(this.s3Metadata.getUserMetadata());
  }

  /**
   * @return returns the size of object in bytes
   */
  @Override
  public long getContentLength() {
    return this.s3Metadata.getContentLength();
  }

  /**
   * Sets size of the associated object in bytes
   *
   * @param contentLength the length of content
   */
  @Override
  public void setContentLength(long contentLength) {
    this.s3Metadata.setContentLength(contentLength);
  }

  /**
   * Adds the key value pair of custom user-metadata for the associated object.
   *
   * @param key the key of user metadata
   * @param value the value of user metadata
   */
  @Override
  public void addUserMetadata(String key, String value) {
    this.s3Metadata.addUserMetadata(key, value);
  }

  /**
   * Sets the optional Content-Encoding HTTP header specifying what content encodings, have been
   * applied to the object and what decoding mechanisms must be applied, in order to obtain the
   * media-type referenced by the Content-Type field.
   *
   * @param encoding the encoding name using in HTTP header Content-Encoding
   */
  @Override
  public void setContentEncoding(String encoding) {
    this.s3Metadata.setContentEncoding(encoding);
  }

  /*
   * @return returns the content encoding type
   */
  @Override
  public String getContentEncoding() {
    return this.s3Metadata.getContentEncoding();
  }
}
