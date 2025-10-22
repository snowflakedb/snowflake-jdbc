package net.snowflake.client.internal.jdbc.cloud.storage;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.google.cloud.storage.Blob;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

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
   * @param bucket the AWS S3 bucket name
   * @return the ObjectSummary object created
   */
  public static StorageObjectSummary createFromS3ObjectSummary(S3Object objSummary, String bucket) {

    return new StorageObjectSummary(
        bucket,
        objSummary.key(),
        // S3 ETag is not always MD5, but since this code path is only
        // used in skip duplicate files in PUT command, It's not
        // critical to guarantee that it's MD5
        objSummary.eTag(),
        objSummary.size());
  }

  /**
   * Creates a platform-agnostic ObjectSummary from an Azure BlobItem
   *
   * @param blobItem an Azure BlobItem object
   * @return the ObjectSummary object created
   */
  public static StorageObjectSummary createFromAzureBlobItem(BlobItem blobItem, String location)
      throws StorageProviderException {
    try {
      long size;
      String key = blobItem.getName();
      BlobItemProperties blobProperties = blobItem.getProperties();

      byte[] contentMd5 = blobProperties.getContentMd5();
      String md5 = contentMd5 != null ? SnowflakeUtil.byteToHexString(contentMd5) : null;
      size = blobProperties.getContentLength();
      return new StorageObjectSummary(location, key, md5, size);
    } catch (Exception ex) {
      logger.debug("Failed to create StorageObjectSummary from Azure BlobItem: {}", ex);
      throw new StorageProviderException(ex);
    }
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
