/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc.cloud.storage;

import com.amazonaws.services.kms.model.UnsupportedOperationException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.util.Iterator;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Iterator class for ObjectSummary objects on Azure Returns platform-independent instances
 * (StorageObjectSummary)
 *
 * @author lgiakoumakis
 */
public class AzureObjectSummariesIterator implements Iterator<StorageObjectSummary> {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(AzureObjectSummariesIterator.class);
  Iterator<ListBlobItem> itemIterator;

  /*
   * Constructs a summaries iterator object from an iterable derived by a
   * lostBlobs method
   * @param azCloudBlobIterable an iterable set of ListBlobItems
   */
  public AzureObjectSummariesIterator(Iterable<ListBlobItem> azCloudBlobIterable) {
    itemIterator = azCloudBlobIterable.iterator();
  }

  public boolean hasNext() {
    return itemIterator.hasNext();
  }

  public StorageObjectSummary next() {
    ListBlobItem listBlobItem = itemIterator.next();

    if (!(listBlobItem instanceof CloudBlob)) {
      // The only other possible type would a CloudDirectory
      // This should never happen since we are listing items as a flat list
      logger.debug("Unexpected listBlobItem instance type: {}", listBlobItem.getClass());
      throw new IllegalArgumentException("Unexpected listBlobItem instance type");
    }

    return StorageObjectSummary.createFromAzureListBlobItem(listBlobItem);
  }

  public void remove() {
    throw new UnsupportedOperationException("remove() method not supported");
  }
}
