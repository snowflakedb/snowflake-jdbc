package net.snowflake.client.jdbc.cloud.storage;

import java.util.Map;

/**
 * Interface for platform-independent remote storage object metadata, modeled after the S3
 * ObjectMetadata class
 *
 * <p>Only the metadata accessors and mutators used by the Client currently are supported,
 * additional methods should be added as needed
 */
public interface StorageObjectMetadata {
  /**
   * @return returns a Map/key-value pairs of metadata properties
   */
  Map<String, String> getUserMetadata();

  /**
   * @return returns the size of object in bytes
   */
  long getContentLength();

  /**
   * Sets size of the associated object in bytes
   *
   * @param contentLength the length of content
   */
  void setContentLength(long contentLength);

  /**
   * Adds the key value pair of custom user-metadata for the associated object.
   *
   * @param key the key of user metadata
   * @param value the value of user metadata
   */
  void addUserMetadata(String key, String value);

  /**
   * Sets the optional Content-Encoding HTTP header specifying what content encodings, have been
   * applied to the object and what decoding mechanisms must be applied, in order to obtain the
   * media-type referenced by the Content-Type field.
   *
   * @param encoding the encoding name using in HTTP header Content-Encoding
   */
  void setContentEncoding(String encoding);

  /*
   * @return returns the content encoding type
   */
  String getContentEncoding();
}
