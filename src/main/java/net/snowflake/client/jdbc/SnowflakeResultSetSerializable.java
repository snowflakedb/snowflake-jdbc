package net.snowflake.client.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This interface defines Snowflake specific APIs to access the data wrapped in the result set
 * serializable object.
 */
public interface SnowflakeResultSetSerializable {
  // This wraps the required info for retrieving ResultSet
  class ResultSetRetrieveConfig {
    private Properties proxyProperties;
    private String sfFullURL;

    public ResultSetRetrieveConfig(Builder builder) {
      this.proxyProperties = builder.proxyProperties;
      this.sfFullURL = builder.sfFullURL;
    }

    public Properties getProxyProperties() {
      return proxyProperties;
    }

    public String getSfFullURL() {
      return sfFullURL;
    }

    // The inner builder class for ResultSetRetrieveConfig
    public static class Builder {
      private Builder() {}

      private Properties proxyProperties = null;
      private String sfFullURL = null;

      public static Builder newInstance() {
        return new Builder();
      }

      public ResultSetRetrieveConfig build() throws IllegalArgumentException {
        // The SFURL must include protocol like https or http
        if (sfFullURL == null || !sfFullURL.toLowerCase().startsWith("http")) {
          throw new IllegalArgumentException(
              "The SF URL must include protocol. The invalid is: " + sfFullURL);
        }

        return new ResultSetRetrieveConfig(this);
      }

      public Builder setProxyProperties(Properties proxyProperties) {
        this.proxyProperties = proxyProperties;
        return this;
      }

      public Builder setSfFullURL(String sfFullURL) {
        this.sfFullURL = sfFullURL;
        return this;
      }
    }
  }

  /**
   * Get ResultSet from the ResultSet Serializable object so that the user can access the data.
   *
   * <p>This API is used by spark connector from 2.6.0 to 2.8.1. It is deprecated from
   * sc:2.8.2/jdbc:3.12.12 since Sept 2020. It is safe to remove it after Sept 2022.
   *
   * @return a ResultSet which represents for the data wrapped in the object
   * @throws SQLException if an error occurs
   * @deprecated Use {@link #getResultSet(ResultSetRetrieveConfig)} instead
   */
  @Deprecated
  ResultSet getResultSet() throws SQLException;

  /**
   * Get ResultSet from the ResultSet Serializable object so that the user can access the data.
   *
   * <p>This API is used by spark connector from 2.6.0 to 2.8.1. It is deprecated from
   * sc:2.8.2/jdbc:3.12.12 since Sept 2020. It is safe to remove it after Sept 2022.
   *
   * @param info The proxy server information if proxy is necessary.
   * @return a ResultSet which represents for the data wrapped in the object
   * @throws SQLException if an error occurs
   * @deprecated Use {@link #getResultSet(ResultSetRetrieveConfig)} instead
   */
  @Deprecated
  ResultSet getResultSet(Properties info) throws SQLException;

  /**
   * Get ResultSet from the ResultSet Serializable object so that the user can access the data.
   *
   * @param resultSetRetrieveConfig The extra info to retrieve the result set.
   * @return a ResultSet which represents for the data wrapped in the object
   * @throws SQLException if an error occurs
   */
  ResultSet getResultSet(ResultSetRetrieveConfig resultSetRetrieveConfig) throws SQLException;

  /**
   * Retrieve total row count included in the ResultSet Serializable object.
   *
   * @return the total row count from metadata
   * @throws SQLException if an error occurs
   */
  long getRowCount() throws SQLException;

  /**
   * Retrieve compressed data size included in the ResultSet Serializable object.
   *
   * @return the total compressed data size in bytes from metadata
   * @throws SQLException if an error occurs
   */
  long getCompressedDataSizeInBytes() throws SQLException;

  /**
   * Retrieve uncompressed data size included in the ResultSet Serializable object.
   *
   * @return the total uncompressed data size in bytes from metadata
   * @throws SQLException if an error occurs
   */
  long getUncompressedDataSizeInBytes() throws SQLException;
}
