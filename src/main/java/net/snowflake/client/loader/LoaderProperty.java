/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.loader;


/**
 * Configuration parameters for Loader
 */
public enum LoaderProperty
{
  tableName,         // Target table name                                          String
  schemaName,        // Target table schema                                        String
  databaseName,      // Target table database                                      String
  remoteStage,       // Stage to use - "~" is default                              String
  columns,           // List of columns that will be uploaded                      List<String>
  keys,              // List of columns used as keys for updating                  List<String>
  operation,         // UPDATE, DELETE, MODIFY, UPSERT                             Enum Operation
  startTransaction,  // start transaction for the operation                        Boolean
  oneBatch,          // process all data in one batch                              Boolean
  truncateTable,     // delete all data from the table prior to run                Boolean
  executeBefore,     // SQL statement to execute before run                        String
  executeAfter,      // SQL statement to execute after run                         String
  isFirstStartCall,  // skip deleting data. Used in multiple calls of loader.start Boolean
  isLastFinishCall,  // skip commit. Used in multiple calls of loader.finish       Boolean
  batchRowSize,      // Batch row size. Flush queues when it reaches this          Long
  csvFileBucketSize, // File bucket size                                           Long
  csvFileSize,       // File size                                                  Long
  preserveStageFile, // Preserve stage files if error occurs                       Boolean
  useLocalTimezone,  // Use local timezone in converting TIMESTAMP                 Boolean
  copyEmptyFieldAsEmpty, // EMPTY_FIELD_AS_NULL = true by default                  Boolean

  // compatibility parameters
  mapTimeToTimestamp, // map TIME data type to TIMESTAMP. Informatica v1
                      // connector behavior.                                       Boolean

  onError,            // on_error option                                           String
  // test parameters
  testRemoteBadCSV   // TEST: Inject bad CSV in the remote stage                   Boolean
}
