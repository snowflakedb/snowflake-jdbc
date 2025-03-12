package net.snowflake.client.loader;

/** Configuration parameters for Loader */
public enum LoaderProperty {
  tableName, // Target table name                                          String
  schemaName, // Target table schema                                        String
  databaseName, // Target table database                                      String
  remoteStage, // Stage to use - "~" is default                              String
  columns, // List of columns that will be uploaded                      List<String>
  keys, // List of columns used as keys for updating                  List<String>
  operation, // UPDATE, DELETE, MODIFY, UPSERT                             Enum Operation
  startTransaction, // start transaction for the operation                        Boolean
  oneBatch, // process all data in one batch                              Boolean
  truncateTable, // delete all data from the table prior to run                Boolean
  executeBefore, // SQL statement to execute before run                        String
  executeAfter, // SQL statement to execute after run                         String
  isFirstStartCall, // skip deleting data. Used in multiple calls of loader.start Boolean
  isLastFinishCall, // skip commit. Used in multiple calls of loader.finish       Boolean
  batchRowSize, // Batch row size. Flush queues when it reaches this          Long
  onError, // on_error option                                            String
  csvFileBucketSize, // File bucket size. 64 by default.                           Long
  csvFileSize, // File size. 50MB by default.                                Long
  preserveStageFile, // Preserve stage files if error occurs                       Boolean
  useLocalTimezone, // Use local timezone in converting TIMESTAMP                 Boolean
  compressFileByPut, // Compress file by PUT. false by default                     Boolean
  compressDataBeforePut, // Compress data before PUT. true by default              Boolean
  compressLevel, // Compress level: 1 (Speed) to 9 (Compression) for
  // compressDataBeforePut option. No impact to
  // compressFileByPut.  1 by default.                          Long

  // compatibility parameters
  mapTimeToTimestamp, // map TIME data type to TIMESTAMP. Informatica v1
  // connector behavior.                                       Boolean

  // deprecated. to be removed.
  copyEmptyFieldAsEmpty, // EMPTY_FIELD_AS_NULL = true by default                  Boolean

  // test parameters
  testRemoteBadCSV // TEST: Inject bad CSV in the remote stage                   Boolean
}
