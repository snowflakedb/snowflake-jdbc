package net.snowflake.client.api.connection;

/**
 * Optional configuration for downloading files from a Snowflake stage as a stream.
 *
 * <p>This class provides optional configuration for the {@link
 * SnowflakeConnection#downloadStream(String, String, DownloadStreamConfig)} method. Required
 * parameters (stageName, sourceFileName) are passed as method arguments, while optional settings
 * are configured here.
 *
 * <p><b>Example usage:</b>
 *
 * <pre>{@code
 * DownloadStreamConfig config = DownloadStreamConfig.builder()
 *     .setDecompress(true)
 *     .build();
 *
 * try (InputStream stream = connection.downloadStream("@my_stage", "data/file.csv.gz", config)) {
 *   // Process the stream
 * }
 * }</pre>
 *
 * @see SnowflakeConnection#downloadStream(String, String, DownloadStreamConfig)
 */
public class DownloadStreamConfig {
  private final boolean decompress;

  /**
   * Private constructor. Use {@link Builder} to create instances.
   *
   * @param builder the builder instance
   */
  private DownloadStreamConfig(Builder builder) {
    this.decompress = builder.decompress;
  }

  /**
   * Whether to decompress the file during download.
   *
   * @return true if the file should be decompressed, false otherwise
   */
  public boolean isDecompress() {
    return decompress;
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
   * Builder for creating {@link DownloadStreamConfig} instances.
   *
   * <p>This builder provides a fluent API for configuring optional download stream settings. All
   * setter methods return the builder instance for method chaining.
   *
   * <p><b>Example:</b>
   *
   * <pre>{@code
   * DownloadStreamConfig config = DownloadStreamConfig.builder()
   *     .setDecompress(true)
   *     .build();
   * }</pre>
   */
  public static class Builder {
    private boolean decompress = false;

    /** Private constructor. Use {@link DownloadStreamConfig#builder()} instead. */
    private Builder() {}

    /**
     * Sets whether to automatically decompress the file during download.
     *
     * <p>If set to {@code true}, the driver will automatically decompress files with recognized
     * compression extensions (e.g., .gz, .bz2, .zip) during download. The returned stream will
     * contain the decompressed data.
     *
     * <p>If set to {@code false}, the file is downloaded as-is without decompression.
     *
     * @param decompress true to decompress the file, false to download as-is
     * @return this builder instance
     */
    public Builder setDecompress(boolean decompress) {
      this.decompress = decompress;
      return this;
    }

    /**
     * Builds the {@link DownloadStreamConfig} instance.
     *
     * @return a new {@link DownloadStreamConfig} instance
     */
    public DownloadStreamConfig build() {
      return new DownloadStreamConfig(this);
    }
  }

  @Override
  public String toString() {
    return "DownloadStreamConfig{" + "decompress=" + decompress + '}';
  }
}
