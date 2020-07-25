/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.cloud.storage;

import com.amazonaws.services.s3.model.ObjectMetadata;
import java.util.Map;

/**
 * s3 implementation of platform independent StorageObjectMetadata interface, wraps an S3
 * ObjectMetadata class
 *
 * <p>It only supports a limited set of metadata properties currrently used by the JDBC client
 *
 * @author lgiakoumakis
 */
public class S3ObjectMetadata implements StorageObjectMetadata {
  private ObjectMetadata objectMetadata;

  S3ObjectMetadata() {
    objectMetadata = new ObjectMetadata();
  }

  // Construct from an AWS S3 ObjectMetadata object
  S3ObjectMetadata(ObjectMetadata meta) {
    objectMetadata = meta;
  }

  @Override
  public Map<String, String> getUserMetadata() {
    return objectMetadata.getUserMetadata();
  }

  @Override
  public long getContentLength() {
    return objectMetadata.getContentLength();
  }

  @Override
  public void setContentLength(long contentLength) {
    objectMetadata.setContentLength(contentLength);
  }

  @Override
  public void addUserMetadata(String key, String value) {
    objectMetadata.addUserMetadata(key, value);
  }

  @Override
  public void setContentEncoding(String encoding) {
    objectMetadata.setContentEncoding(encoding);
  }

  @Override
  public String getContentEncoding() {
    return objectMetadata.getContentEncoding();
  }

  /** @return Returns the encapsulated AWS S3 metadata object */
  ObjectMetadata getS3ObjectMetadata() {
    return objectMetadata;
  }
}
