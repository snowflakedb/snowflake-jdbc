/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc.cloud.storage;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.Iterator;
import java.util.List;


/**
 * Cloud platform agnostic class that provides and iterator over storage object summaries
 *
 * @author lgiakoumakis
 *
 */
public class StorageObjectSummaryCollection implements Iterable<StorageObjectSummary> {

  private List<S3ObjectSummary> s3ObjSummariesList = null;
  private enum storageType {S3};
  private final storageType sType;

  // Constructs platform-agnostic collection of object summaries from S3 object summaries
  public StorageObjectSummaryCollection (List<S3ObjectSummary> s3ObjectSummaries)
  {
    this.s3ObjSummariesList = s3ObjectSummaries;
    sType = storageType.S3;
  }

  public Iterator<StorageObjectSummary> iterator()
  {
    if(sType==storageType.S3)
    {
      return new S3ObjectSummariesIterator(s3ObjSummariesList);
    }
    else throw new IllegalArgumentException("Unspecified storage provider");

  }
}

