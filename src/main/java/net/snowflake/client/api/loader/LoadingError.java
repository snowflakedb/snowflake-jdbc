package net.snowflake.client.api.loader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.internal.loader.BufferStage;
import net.snowflake.client.internal.loader.StreamLoader;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

/**
 * Wrapper for data format errors returned by the COPY/validate command.
 *
 * <p>This class encapsulates error information from failed data loading operations. It provides
 * details about what went wrong, where in the file the error occurred, and the rejected record
 * data.
 *
 * <h2 id="usage-example">Usage Example</h2>
 *
 * <pre>{@code
 * LoadResultListener listener = new LoadResultListener() {
 *   public void addError(LoadingError error) {
 *     System.err.println("Error in file: " + error.getFile());
 *     System.err.println("Line: " + error.getProperty(ErrorProperty.LINE));
 *     System.err.println("Error: " + error.getProperty(ErrorProperty.ERROR));
 *   }
 *   // ... implement other methods
 * };
 * }</pre>
 *
 * @see LoadResultListener
 * @see Loader
 */
// this could be internal?
public class LoadingError {
  private static final SFLogger logger = SFLoggerFactory.getLogger(LoadingError.class);

  /**
   * Properties that can be associated with a loading error.
   *
   * <p>These correspond to columns in the Snowflake COPY command validation results and provide
   * detailed information about what went wrong during data loading.
   */
  public enum ErrorProperty {
    /** The error message describing what went wrong */
    ERROR,
    /** The line number in the source file where the error occurred */
    LINE,
    /** The character position where the error occurred */
    CHARACTER,
    /** The byte offset in the file where the error occurred */
    BYTE_OFFSET,
    /** The error category (e.g., "parsing", "data type mismatch") */
    CATEGORY,
    /** The numeric error code */
    CODE,
    /** The SQL state code */
    SQL_STATE,
    /** The name of the column where the error occurred */
    COLUMN_NAME,
    /** The row number in the result set */
    ROW_NUMBER,
    /** The starting line number of the row */
    ROW_START_LINE,
    /** The rejected record data */
    REJECTED_RECORD
  }

  private String _stage;

  private String _prefix;

  private String _file;

  private final String _target;

  private final Map<ErrorProperty, String> _properties = new HashMap<ErrorProperty, String>();

  public static String UNKNOWN = "unknown";

  /**
   * Construct error from validation output
   *
   * @param rs result set
   * @param bs buffer stage
   * @param loader stream loader
   */
  public LoadingError(ResultSet rs, BufferStage bs, StreamLoader loader) {
    _stage = bs.getRemoteLocation();

    try {
      String ffile = rs.getString("FILE");
      _file = ffile.substring(ffile.lastIndexOf("/"));
      _prefix = ffile.substring(0, ffile.lastIndexOf("/"));
    } catch (SQLException ex) {
      _file = UNKNOWN;
    }

    _target = loader.getTable();

    for (ErrorProperty p : ErrorProperty.values()) {
      try {
        _properties.put(p, rs.getString(p.name()));
      } catch (SQLException ex) {
        logger.error("Exception", ex);
      }
    }
  }

  /**
   * Gets the stage name where the error occurred.
   *
   * @return the stage name
   */
  public String getStage() {
    return _stage;
  }

  /**
   * Gets the file prefix within the stage.
   *
   * @return the file prefix path
   */
  public String getPrefix() {
    return _prefix;
  }

  /**
   * Gets the file name where the error occurred.
   *
   * @return the file name
   */
  public String getFile() {
    return _file;
  }

  /**
   * Gets the target table name.
   *
   * @return the target table name
   */
  public String getTarget() {
    return _target;
  }

  /**
   * Gets the value of a specific error property.
   *
   * @param p the error property to retrieve
   * @return the value of the error property, or null if not set
   */
  public String getProperty(ErrorProperty p) {
    return this._properties.get(p);
  }

  /**
   * Sets the value of a specific error property.
   *
   * @param p the error property to set
   * @param value the value to assign to the property
   */
  public void setProperty(ErrorProperty p, String value) {
    this._properties.put(p, value);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("{");
    String prefix = "";
    for (ErrorProperty p : ErrorProperty.values()) {
      sb.append(prefix);
      sb.append("\"").append(p.name()).append("\": ");
      sb.append("\"");
      String value = String.valueOf(_properties.get(p));
      sb.append(value.replaceAll("[\\s]+", " ").replace("\"", "\\\""));
      sb.append("\"");
      prefix = ",";
    }
    sb.append("}");

    return sb.toString();
  }

  /**
   * Converts this loading error into a DataError exception.
   *
   * @return a DataError exception containing this error's details
   */
  public Loader.DataError getException() {
    return new Loader.DataError(this.toString());
  }
}
