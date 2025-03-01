package net.snowflake.client.jdbc.cloud.storage;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.cloud.storage.Blob;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.net.URISyntaxException;
import java.util.Base64;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/** Storage platform agnostic class that encapsulates remote storage object properties */
public class StorageObjectSummary {
  private static final SFLogger logger = SFLoggerFactory.getLogger(StorageObjectSummary.class);
  private String location; // location translates to "bucket" for S3
  private String key;
  private String md5;
  private long size;

  /**
   * Constructs a StorageObjectSummary object from the S3 equivalent S3ObjectSummary
   *
   * @param location Location of the S3 object
   * @param key Key of the S3Object
   * @param md5 The MD5 hash of the object
   * @param size The size of the S3 object
   */
  private StorageObjectSummary(String location, String key, String md5, long size) {
    this.location = location;
    this.key = key;
    this.md5 = md5;
    this.size = size;
  }

  /**
   * Constructs a StorageObjectSummary object from the S3 equivalent S3ObjectSummary
   *
   * @param objSummary the AWS S3 ObjectSummary object to copy from
   * @return the ObjectSummary object created
   */
  public static StorageObjectSummary createFromS3ObjectSummary(S3ObjectSummary objSummary) {

    return new StorageObjectSummary(
        objSummary.getBucketName(),
        objSummary.getKey(),
        // S3 ETag is not always MD5, but since this code path is only
        // used in skip duplicate files in PUT command, It's not
        // critical to guarantee that it's MD5
        objSummary.getETag(),
        objSummary.getSize());
  }

  /**
   * Constructs a StorageObjectSummary object from Azure BLOB properties Using factory methods to
   * create these objects since Azure can throw, while retrieving the BLOB properties
   *
   * @param listBlobItem an Azure ListBlobItem object
   * @return the ObjectSummary object created
   */
  public static StorageObjectSummary createFromAzureListBlobItem(ListBlobItem listBlobItem)
      throws StorageProviderException {
    String location, key, md5;
    long size;
    CloudBlobContainer container;

    // Retrieve the BLOB properties that we need for the Summary
    // Azure Storage stores metadata inside each BLOB, therefore the listBlobItem
    // will point us to the underlying BLOB and will get the properties from it
    // During the process the Storage Client could fail, hence we need to wrap the
    // get calls in try/catch and handle possible exceptions
    try {
      container = listBlobItem.getContainer();
      location = container.getName();
      CloudBlob cloudBlob = (CloudBlob) listBlobItem;
      key = cloudBlob.getName();
      BlobProperties blobProperties = cloudBlob.getProperties();
      // the content md5 property is not always the actual md5 of the file. But for here, it's only
      // used for skipping file on PUT command, hence is ok.
      md5 = convertBase64ToHex(blobProperties.getContentMD5());
      size = blobProperties.getLength();
    } catch (URISyntaxException | StorageException ex) {
      // This should only happen if somehow we got here with and invalid URI (it should never
      // happen)
      // ...or there is a Storage service error. Unlike S3, Azure fetches metadata from the BLOB
      // itself,
      // and its a lazy operation
      logger.debug("Failed to create StorageObjectSummary from Azure ListBlobItem: {}", ex);
      throw new StorageProviderException(ex);
    } catch (Throwable th) {
      logger.debug("Failed to create StorageObjectSummary from Azure ListBlobItem: {}", th);
      throw th;
    }
    return new StorageObjectSummary(location, key, md5, size);
  }

  /**
   * createFromGcsBlob creates a StorageObjectSummary from a GCS blob object
   *
   * @param blob GCS blob object
   * @return a new StorageObjectSummary
   */
  public static StorageObjectSummary createFromGcsBlob(Blob blob) {
    String bucketName = blob.getBucket();
    String path = blob.getName();
    String hexMD5 = blob.getMd5ToHexString();
    long size = blob.getSize();
    return new StorageObjectSummary(bucketName, path, hexMD5, size);
  }

  private static String convertBase64ToHex(String base64String) {
    try {
      byte[] bytes = Base64.getDecoder().decode(base64String);

      final StringBuilder builder = new StringBuilder();
      for (byte b : bytes) {
        builder.append(String.format("%02x", b));
      }
      return builder.toString();
      // return empty string if input is not a valid Base64 string
    } catch (Exception e) {
      return "";
    }
  }

  /**
   * @return returns the location of the object
   */
  public String getLocation() {
    return location;
  }

  /**
   * @return returns the key property of the object
   */
  public String getKey() {
    return key;
  }

  /**
   * @return returns the MD5 hash of the object
   */
  public String getMD5() {
    return md5;
  }

  /**
   * @return returns the size property of the object
   */
  public long getSize() {
    return size;
  }
}
