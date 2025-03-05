package net.snowflake.client.jdbc.cloud.storage;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import java.util.Iterator;

/**
 * Iterator class for ObjectSummary objects on GCS objects. Returns platform-independent instances
 * (StorageObjectSummary)
 */
public class GcsObjectSummariesIterator implements Iterator<StorageObjectSummary> {
  private final Iterator<Blob> blobIterator;

  public GcsObjectSummariesIterator(Page<Blob> blobs) {
    this.blobIterator = blobs.iterateAll().iterator();
  }

  @Override
  public boolean hasNext() {
    return this.blobIterator.hasNext();
  }

  @Override
  public StorageObjectSummary next() {
    Blob blob = this.blobIterator.next();
    return StorageObjectSummary.createFromGcsBlob(blob);
  }
}
