package net.snowflake.client.internal.core.minicore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class MinicoreLoadResult {

  private final boolean success;
  private final String errorMessage;
  private final String libraryFileName;
  private final MinicoreLibrary library;
  private final String coreVersion;
  private final Throwable exception;
  private final List<String> logs;

  private MinicoreLoadResult(
      boolean success,
      String errorMessage,
      String libraryFileName,
      MinicoreLibrary library,
      String coreVersion,
      Throwable exception,
      List<String> logs) {
    this.success = success;
    this.errorMessage = errorMessage;
    this.libraryFileName = libraryFileName;
    this.library = library;
    this.coreVersion = coreVersion;
    this.exception = exception;
    this.logs = logs != null ? logs : new ArrayList<>();
  }

  public static MinicoreLoadResult success(
      String libraryFileName, MinicoreLibrary library, String coreVersion, List<String> logs) {
    return new MinicoreLoadResult(true, null, libraryFileName, library, coreVersion, null, logs);
  }

  public static MinicoreLoadResult failure(
      String errorMessage, String libraryFileName, Throwable exception, List<String> logs) {
    return new MinicoreLoadResult(
        false, errorMessage, libraryFileName, null, null, exception, logs);
  }

  public boolean isSuccess() {
    return success;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public Throwable getException() {
    return exception;
  }

  public String getLibraryFileName() {
    return libraryFileName;
  }

  public MinicoreLibrary getLibrary() {
    return library;
  }

  public String getCoreVersion() {
    return coreVersion;
  }

  public List<String> getLogs() {
    return Collections.unmodifiableList(logs);
  }

  @Override
  public String toString() {
    if (success) {
      return String.format(
          "MinicoreLoadResult{success=true, libraryFileName='%s', version='%s'}",
          libraryFileName, coreVersion);
    } else {
      return String.format(
          "MinicoreLoadResult{success=false, error='%s', exception=%s}",
          errorMessage, exception != null ? exception.getClass().getSimpleName() : "none");
    }
  }
}
