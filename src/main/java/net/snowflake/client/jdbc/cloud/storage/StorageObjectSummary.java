/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc.cloud.storage;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

import java.net.URISyntaxException;

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
   * Contructs a StorageObjectSummary object from the S3 equivalent S3ObjectSummary
   * @param
   */
  private StorageObjectSummary(String location, String key, String eTag, long size)
  {
    this.location = location;
    this.key = key;
    this.eTag = eTag;
    this.size = size;
  }

  /**
   * Contructs a StorageObjectSummary object from the S3 equivalent S3ObjectSummary
   * @param objSummary the AWS S3 ObjectSummary object to copy from
   * @return the ObjectSummary object created
   */
 public static StorageObjectSummary createFromS3ObjectSummary(S3ObjectSummary objSummary)
  {

    return new StorageObjectSummary(
                  objSummary.getBucketName(),
                  objSummary.getKey(),
                  objSummary.getETag(),
                  objSummary.getSize()
                  );
  }

  /**
   * Contructs a StorageObjectSummary object from Azure BLOB properties
   * Using factory methods to create these objects since Azure can throw,
   * while retrieving the BLOB properties
   * @param listBlobItem an Azure ListBlobItem object
   * @return the ObjectSummary object created
   */
  public static StorageObjectSummary createFromAzureListBlobItem(ListBlobItem listBlobItem)
    throws StorageProviderException
  {
    String location, key, eTag;
    long size;

    // Retrieve the BLOB properties that we need for the Summary
    // Azure Storage stores metadata inside each BLOB, therefore the listBlobItem
    // will point us to the underlying BLOB and will get the properties from it
    // During the process the Storage Client could fail, hence we need to wrap the
    // get calls in try/catch and handle possible exceptions
    try
    {
      location = listBlobItem.getContainer().getName();

      CloudBlob cloudBlob = (CloudBlob) listBlobItem;
      key = cloudBlob.getName();

      BlobProperties blobProperties = cloudBlob.getProperties();
      eTag = blobProperties.getEtag();
      size = blobProperties.getLength();
    }
    catch (URISyntaxException | StorageException ex)
    {
      // This should only happen if somehow we got here with and invalid URI (it should never happen)
      // ...or there is a Storage service error. Unlike S3, Azure fetches metadata from the BLOB itself,
      // and its a lazy operation
      throw new StorageProviderException(ex);
    }
    return new StorageObjectSummary(location, key, eTag, size);

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
