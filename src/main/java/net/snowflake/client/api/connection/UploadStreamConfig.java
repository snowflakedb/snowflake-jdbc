package net.snowflake.client.api.connection;

/**
 * Optional configuration for uploading data to a Snowflake stage from a stream.
 *
 * <p>This class provides optional configuration for the {@link
 * SnowflakeConnection#uploadStream(String, String, java.io.InputStream, UploadStreamConfig)}
 * method. Required parameters (stageName, destFileName, inputStream) are passed as method
 * arguments, while optional settings are configured here.
 *
 * <p><b>Example usage:</b>
 *
 * <pre>{@code
 * try (InputStream dataStream = new FileInputStream("data.csv")) {
 *   UploadStreamConfig config = UploadStreamConfig.builder()
 *       .setDestPrefix("data/2024")
 *       .setCompressData(true)
 *       .build();
 *
 *   connection.uploadStream("@my_stage", "uploaded_data.csv", dataStream, config);
 * }
 * }</pre>
 *
 * @see SnowflakeConnection#uploadStream(String, String, java.io.InputStream, UploadStreamConfig)
 */
public class UploadStreamConfig {
  private final String destPrefix;
  private final boolean compressData;

  /**
   * Private constructor. Use {@link Builder} to create instances.
   *
   * @param builder the builder instance
   */
  private UploadStreamConfig(Builder builder) {
    this.destPrefix = builder.destPrefix;
    this.compressData = builder.compressData;
  }

  /**
   * Gets the destination prefix (directory path within the stage).
   *
   * @return the destination prefix, or null if files should be uploaded to stage root
   */
  public String getDestPrefix() {
    return destPrefix;
  }

  /**
   * Whether to compress the data during upload.
   *
   * @return true if data should be compressed, false otherwise
   */
  public boolean isCompressData() {
    return compressData;
  }

  /**
   * Creates a new builder instance.
   *
   * @return a new {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for creating {@link UploadStreamConfig} instances.
   *
   * <p>This builder provides a fluent API for configuring optional upload stream settings. All
   * setter methods return the builder instance for method chaining.
   *
   * <p><b>Example:</b>
   *
   * <pre>{@code
   * UploadStreamConfig config = UploadStreamConfig.builder()
   *     .setDestPrefix("data/2024")
   *     .setCompressData(true)
   *     .build();
   * }</pre>
   */
  public static class Builder {
    private String destPrefix;
    private boolean compressData = true;

    /** Private constructor. Use {@link UploadStreamConfig#builder()} instead. */
    private Builder() {}

    /**
     * Sets the destination prefix (directory path) within the stage.
     *
     * <p>This is optional. If not set, files will be uploaded to the root of the stage. Use forward
     * slashes to separate directory levels.
     *
     * <p><b>Examples:</b>
     *
     * <ul>
     *   <li>{@code "data"} - upload to data directory
     *   <li>{@code "data/2024/01"} - upload to nested directories
     *   <li>{@code null} or empty - upload to stage root (default)
     * </ul>
     *
     * @param destPrefix the destination prefix/directory path (can be null or empty for stage root)
     * @return this builder instance
     */
    public Builder setDestPrefix(String destPrefix) {
      this.destPrefix =
          (destPrefix == null || destPrefix.trim().isEmpty()) ? null : destPrefix.trim();
      return this;
    }

    /**
     * Sets whether to automatically compress the data during upload.
     *
     * <p>If set to {@code true} (default), the driver will compress the data using gzip compression
     * before uploading. This reduces upload time and storage costs. The file will be stored with a
     * .gz extension appended to the destination file name.
     *
     * <p>If set to {@code false}, the data is uploaded as-is without compression.
     *
     * @param compressData true to compress the data (default), false to upload uncompressed
     * @return this builder instance
     */
    public Builder setCompressData(boolean compressData) {
      this.compressData = compressData;
      return this;
    }

    /**
     * Builds the {@link UploadStreamConfig} instance.
     *
     * @return a new {@link UploadStreamConfig} instance
     */
    public UploadStreamConfig build() {
      return new UploadStreamConfig(this);
    }
  }

  @Override
  public String toString() {
    return "UploadStreamConfig{"
        + "destPrefix='"
        + destPrefix
        + '\''
        + ", compressData="
        + compressData
        + '}';
  }
}
