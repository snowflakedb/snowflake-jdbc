/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc.cloud.storage;

import com.amazonaws.services.kms.model.UnsupportedOperationException;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.Iterator;
import java.util.List;

/**
 * Iterator class for ObjectSummary objects on S3 Wraps an iterator of S3 object summaries and
 * returns platform independent instances (StorageObjectSummary)
 *
 * @author lgiakoumakis
 */
public class S3ObjectSummariesIterator implements Iterator<StorageObjectSummary> {

  // Encapsulated S3 iterator
  private Iterator<S3ObjectSummary> s3ObjSummariesIterator;

  /*
   * Constructs a summaries interator object from S3Object summary list
   * derived from the AWS client
   * @param s3ObjectSummaries a list of S3ObjectSummaries to construct from
   */
  public S3ObjectSummariesIterator(List<S3ObjectSummary> s3ObjectSummaries) {
    s3ObjSummariesIterator = s3ObjectSummaries.iterator();
  }

  public boolean hasNext() {
    return s3ObjSummariesIterator.hasNext();
  }

  public StorageObjectSummary next() {
    // Get the next S3 summary object and return it as a platform-agnostic object
    // (StorageObjectSummary)
    S3ObjectSummary s3Obj = s3ObjSummariesIterator.next();

    return StorageObjectSummary.createFromS3ObjectSummary(s3Obj);
  }

  public void remove() {
    throw new UnsupportedOperationException("remove() method not supported");
  }
}
