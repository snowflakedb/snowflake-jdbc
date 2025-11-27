package net.snowflake.client.internal.core.minicore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class MinicoreLoadResult {

  private final boolean success;
  private final String errorMessage;
  private final String loadedFromPath;
  private final Throwable exception;
  private final List<String> logs;

  private MinicoreLoadResult(
      boolean success,
      String errorMessage,
      String loadedFromPath,
      Throwable exception,
      List<String> logs) {
    this.success = success;
    this.errorMessage = errorMessage;
    this.loadedFromPath = loadedFromPath;
    this.exception = exception;
    this.logs = logs != null ? new ArrayList<>(logs) : new ArrayList<>();
  }

  public static MinicoreLoadResult success(String loadedFromPath, List<String> logs) {
    return new MinicoreLoadResult(true, null, loadedFromPath, null, logs);
  }

  public static MinicoreLoadResult failure(String errorMessage, Throwable exception, List<String> logs) {
    return new MinicoreLoadResult(false, errorMessage, null, exception, logs);
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

  public List<String> getLogs() {
    return Collections.unmodifiableList(logs);
  }

  @Override
  public String toString() {
    if (success) {
      return String.format(
          "MinicoreLoadResult{success=true, loadedFrom='%s'}",
          loadedFromPath);
    } else {
      return String.format(
          "MinicoreLoadResult{success=false, error='%s', exception=%s}",
          errorMessage, exception != null ? exception.getClass().getSimpleName() : "none");
    }
  }
}
