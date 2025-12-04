package net.snowflake.client.internal.jdbc.cloud.storage;

import com.azure.storage.blob.models.BlobItem;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import java.util.Iterator;
import java.util.List;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Provides and iterator over storage object summaries from all supported cloud storage providers
 */
public class StorageObjectSummaryCollection implements Iterable<StorageObjectSummary> {

  private enum storageType {
    S3,
    AZURE,
    GCS
  };

  private final storageType sType;
  private List<S3Object> s3ObjSummariesList = null;
  private Iterable<BlobItem> azCLoudBlobIterable = null;
  private Page<Blob> gcsIterablePage = null;
  // explicitly store bucket name for S3 because S3Object does not contain bucket info
  private String bucketName = null;

  // Constructs platform-agnostic collection of object summaries from S3 objects
  public StorageObjectSummaryCollection(List<S3Object> s3ObjectSummaries, String bucketName) {
    this.s3ObjSummariesList = s3ObjectSummaries;
    sType = storageType.S3;
    this.bucketName = bucketName;
  }

  // Constructs platform-agnostic collection of object summaries from an Azure CloudBlobDirectory
  // object
  public StorageObjectSummaryCollection(Iterable<BlobItem> azCLoudBlobIterable, String bucketName) {
    this.azCLoudBlobIterable = azCLoudBlobIterable;
    sType = storageType.AZURE;
    this.bucketName = bucketName;
  }

  public StorageObjectSummaryCollection(Page<Blob> gcsIterablePage) {
    this.gcsIterablePage = gcsIterablePage;
    sType = storageType.GCS;
  }

  @Override
  public Iterator<StorageObjectSummary> iterator() {
    switch (sType) {
      case S3:
        return new S3ObjectSummariesIterator(s3ObjSummariesList, this.bucketName);
      case AZURE:
        return new AzureObjectSummariesIterator(azCLoudBlobIterable, bucketName);
      case GCS:
        return new GcsObjectSummariesIterator(this.gcsIterablePage);
      default:
        throw new IllegalArgumentException("Unspecified storage provider");
    }
  }
}
