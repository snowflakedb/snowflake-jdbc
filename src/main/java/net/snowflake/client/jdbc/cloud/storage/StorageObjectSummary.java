/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc.cloud.storage;

import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Storage platform agnostic class that encapsulates remote storage object properties
 *
 * @author lgiakoumakis
 */
public class StorageObjectSummary
{
  private String location;      // location translates to "bucket" for S3
  private String key;
  private String eTag;
  private long size;

  /**
   * Contructs a ObjectSummary object from the S3 equivalent
   * @param objSummary the AWS S3 ObjectSummary object to copy from
   */
  StorageObjectSummary(S3ObjectSummary objSummary)
  {
    location = objSummary.getBucketName();
    key = objSummary.getKey();
    eTag = objSummary.getETag();
    size = objSummary.getSize();
  }

  /**
   * @return returns the location of the object
   */
  public String getLocation()
  {
    return location;
  }

  /**
   * @return returns the key property of the object
   */
  public String getKey() {
    return key;
  }

  /**
   * @return returns the ETag property of the object
   */
  public String getETag()
  {
    return eTag;
  }

  /**
   * @return returns the size property of the object
   */
  public long getSize()
  {
    return size;
  }
}
