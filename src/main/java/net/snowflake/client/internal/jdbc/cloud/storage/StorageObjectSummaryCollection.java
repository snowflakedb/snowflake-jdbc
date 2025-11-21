package net.snowflake.client.internal.jdbc.cloud.storage;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.azure.storage.blob.models.BlobItem;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import java.util.Iterator;
import java.util.List;

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
  private List<S3ObjectSummary> s3ObjSummariesList = null;
  private Iterable<BlobItem> azCLoudBlobIterable = null;
  private Page<Blob> gcsIterablePage = null;
  private String location; // bucket name

  // Constructs platform-agnostic collection of object summaries from S3 object summaries
  public StorageObjectSummaryCollection(List<S3ObjectSummary> s3ObjectSummaries) {
    this.s3ObjSummariesList = s3ObjectSummaries;
    sType = storageType.S3;
  }

  // Constructs platform-agnostic collection of object summaries from an Azure CloudBlobDirectory
  // object
  public StorageObjectSummaryCollection(Iterable<BlobItem> azCLoudBlobIterable, String location) {
    this.azCLoudBlobIterable = azCLoudBlobIterable;
    sType = storageType.AZURE;
    this.location = location;
  }

  public StorageObjectSummaryCollection(Page<Blob> gcsIterablePage) {
    this.gcsIterablePage = gcsIterablePage;
    sType = storageType.GCS;
  }

  @Override
  public Iterator<StorageObjectSummary> iterator() {
    switch (sType) {
      case S3:
        return new S3ObjectSummariesIterator(s3ObjSummariesList);
      case AZURE:
        return new AzureObjectSummariesIterator(azCLoudBlobIterable, location);
      case GCS:
        return new GcsObjectSummariesIterator(this.gcsIterablePage);
      default:
        throw new IllegalArgumentException("Unspecified storage provider");
    }
  }
}
