package net.snowflake.client.loader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/** Wrapper for data format errors returned by the COPY/validate command */
public class LoadingError {
  private static final SFLogger logger = SFLoggerFactory.getLogger(LoadingError.class);

  public enum ErrorProperty {
    ERROR,
    LINE,
    CHARACTER,
    BYTE_OFFSET,
    CATEGORY,
    CODE,
    SQL_STATE,
    COLUMN_NAME,
    ROW_NUMBER,
    ROW_START_LINE,
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

  public String getStage() {
    return _stage;
  }

  public String getPrefix() {
    return _prefix;
  }

  public String getFile() {
    return _file;
  }

  public String getTarget() {
    return _target;
  }

  public String getProperty(ErrorProperty p) {
    return this._properties.get(p);
  }

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

  public Loader.DataError getException() {
    return new Loader.DataError(this.toString());
  }
}
