package net.snowflake.client.loader;

/**
 * Represents actions to perform when an error is encountered while loading data from a file.
 */
public enum CopyIntoOnError {

    CONTINUE,           // Continue loading the file
    SKIP_FILE,          // Skip file if any errors encountered in the file
    ABORT_STATEMENT     // Abort the COPY statement if any error is encountered

}
