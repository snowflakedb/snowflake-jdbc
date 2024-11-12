/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc.cloud.storage;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.azure.storage.blob.models.BlobItem;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.microsoft.azure.storage.blob.ListBlobItem;

import java.util.Iterator;
import java.util.List;

/**
 * Provides and iterator over storage object summaries from all supported cloud storage providers
 *
 * @author lgiakoumakis
 */
public class StorageObjectSummaryCollection implements Iterable<StorageObjectSummary> {

  private String azStorageLocation = null;

  private enum storageType {
    S3,
    AZURE,
    GCS
  };

  private final storageType sType;
  private List<S3ObjectSummary> s3ObjSummariesList = null;
  private Iterable<ListBlobItem> azCLoudBlobIterable = null;
  private Iterable<BlobItem> azCloudBlobItemIterable = null;
  private Page<Blob> gcsIterablePage = null;

  // Constructs platform-agnostic collection of object summaries from S3 object summaries
  public StorageObjectSummaryCollection(List<S3ObjectSummary> s3ObjectSummaries) {
    this.s3ObjSummariesList = s3ObjectSummaries;
    sType = storageType.S3;
  }

  // Constructs platform-agnostic collection of object summaries from an Azure CloudBlobDirectory
  // object
  public StorageObjectSummaryCollection(Iterable<BlobItem> azCLoudBlobItemIterable, String remoteStorageLocation) {
    this.azCloudBlobItemIterable = azCLoudBlobItemIterable;
    this.azStorageLocation = remoteStorageLocation;
    sType = storageType.AZURE;
  }

  public StorageObjectSummaryCollection(Iterable<ListBlobItem> azCLoudBlobIterable) {
    this.azCLoudBlobIterable = azCLoudBlobIterable;
    sType = storageType.AZURE;
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
        if (azStorageLocation == null) {
          if (azCLoudBlobIterable != null) {
            return new AzureObjectSummariesIterator(azCLoudBlobIterable);
          }
          throw new RuntimeException("Storage type is Azure but azStorageLocation field is not set. Should never happen");
        }
        return new AzureObjectSummariesIterator(azCloudBlobItemIterable, azStorageLocation);
      case GCS:
        return new GcsObjectSummariesIterator(this.gcsIterablePage);
      default:
        throw new IllegalArgumentException("Unspecified storage provider");
    }
  }
}
