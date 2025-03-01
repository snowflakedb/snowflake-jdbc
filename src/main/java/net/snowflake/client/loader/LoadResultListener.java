package net.snowflake.client.loader;

/** Callback API for processing errors and statistics of upload operation */
public interface LoadResultListener {

  /**
   * @return this result listener needs to listen to the provided records
   */
  boolean needSuccessRecords();

  /**
   * @param op Operation requested
   * @param record Data submitted for processing
   */
  void recordProvided(Operation op, Object[] record);

  /**
   * @param op Operation requested
   * @param i number of rows that had been processed
   */
  void addProcessedRecordCount(Operation op, int i);

  /**
   * @param op Operation requested
   * @param i number of rows that had been affected by a given operation
   */
  void addOperationRecordCount(Operation op, int i);

  /**
   * @return whether this result listener needs to listen to error records
   */
  boolean needErrors();

  /**
   * @param error information about error that was encountered
   */
  void addError(LoadingError error);

  /**
   * @return Whether to throw an exception upon encountering error
   */
  boolean throwOnError();

  /**
   * Method to add to the error count for a listener
   *
   * @param number the number of errors
   */
  void addErrorCount(int number);

  /** Method to reset the error count back to zero */
  void resetErrorCount();

  /**
   * method to get the total number of errors
   *
   * @return the number of errors
   */
  int getErrorCount();

  /**
   * Method to add to the error record count for a listener
   *
   * @param number the number of error records
   */
  void addErrorRecordCount(int number);

  /** Method to reset the errorRecordCount back to zero */
  void resetErrorRecordCount();

  /**
   * method to get the total number of error records
   *
   * @return the number of rows in errors
   */
  int getErrorRecordCount();

  /** Resets submitted row count */
  void resetSubmittedRowCount();

  /**
   * Adds the number of submitted rows
   *
   * @param number the number of submitted row
   */
  void addSubmittedRowCount(int number);

  /**
   * Gets the number of submitted row
   *
   * @return the number of submitted row
   */
  int getSubmittedRowCount();
}
