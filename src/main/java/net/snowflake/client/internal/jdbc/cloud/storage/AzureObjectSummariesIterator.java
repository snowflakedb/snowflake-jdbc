package net.snowflake.client.internal.jdbc.cloud.storage;

import com.azure.storage.blob.models.BlobItem;
import java.util.Iterator;
import java.util.NoSuchElementException;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

/**
 * Iterator class for ObjectSummary objects on Azure Returns platform-independent instances
 * (StorageObjectSummary)
 */
public class AzureObjectSummariesIterator implements Iterator<StorageObjectSummary> {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(AzureObjectSummariesIterator.class);
  private final Iterator<BlobItem> itemIterator;
  private final String location;

  /*
   * Constructs a summaries iterator object from an iterable derived by a
   * lostBlobs method
   * @param azCloudBlobIterable an iterable set of ListBlobItems
   */
  public AzureObjectSummariesIterator(Iterable<BlobItem> azCloudBlobIterable, String location) {
    itemIterator = azCloudBlobIterable.iterator();
    this.location = location;
  }

  public boolean hasNext() {
    // SNOW-442579 azure itemIterator.hasNext() is a lazy operation, which may cause
    // StorageException. And it seems Azure wraps the StorageException within the
    // NoSuchElementException.
    try {
      return itemIterator.hasNext();
    } catch (NoSuchElementException ex) {
      logger.debug("Failed to run azure iterator.hasNext().", ex);
      throw new StorageProviderException(
          (Exception) ex.getCause()); // ex.getCause() should be StorageException
    }
  }

  public StorageObjectSummary next() {
    BlobItem blobItem = itemIterator.next();

    // In the new Azure SDK, BlobItem is the standard type for blob listings
    // No need to check for CloudBlob vs CloudDirectory as in the old SDK

    return StorageObjectSummary.createFromAzureBlobItem(blobItem, location);
  }

  public void remove() {
    throw new UnsupportedOperationException("remove() method not supported");
  }
}
