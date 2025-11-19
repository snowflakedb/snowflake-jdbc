package net.snowflake.client.api.connection;

/**
 * Configuration for downloading files from a Snowflake stage as a stream.
 *
 * <p>This class provides configuration options for the {@link
 * SnowflakeConnection#downloadStream(DownloadStreamConfig)} method, which allows downloading files
 * from Snowflake internal or external stages.
 *
 * <p><b>Example usage:</b>
 *
 * <pre>{@code
 * DownloadStreamConfig config = DownloadStreamConfig.builder()
 *     .setStageName("@my_stage")
 *     .setSourceFileName("data/file.csv")
 *     .setDecompress(true)
 *     .build();
 *
 * try (InputStream stream = connection.downloadStream(config)) {
 *   // Process the stream
 * }
 * }</pre>
 *
 * @see SnowflakeConnection#downloadStream(DownloadStreamConfig)
 */
public class DownloadStreamConfig {
  private final String stageName;
  private final String sourceFileName;
  private final boolean decompress;

  /**
   * Private constructor. Use {@link Builder} to create instances.
   *
   * @param builder the builder instance
   */
  private DownloadStreamConfig(Builder builder) {
    this.stageName = builder.stageName;
    this.sourceFileName = builder.sourceFileName;
    this.decompress = builder.decompress;
  }

  /**
   * Gets the stage name.
   *
   * @return the stage name (e.g., "@my_stage" or "@~" for user stage)
   */
  public String getStageName() {
    return stageName;
  }

  /**
   * Gets the source file name or path within the stage.
   *
   * @return the source file name/path
   */
  public String getSourceFileName() {
    return sourceFileName;
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
   * <p>This builder provides a fluent API for configuring download stream operations. All setter
   * methods return the builder instance for method chaining.
   *
   * <p><b>Example:</b>
   *
   * <pre>{@code
   * DownloadStreamConfig config = DownloadStreamConfig.builder()
   *     .setStageName("@my_stage")
   *     .setSourceFileName("data/file.csv.gz")
   *     .setDecompress(true)
   *     .build();
   * }</pre>
   */
  public static class Builder {
    private String stageName;
    private String sourceFileName;
    private boolean decompress = false;

    /** Private constructor. Use {@link DownloadStreamConfig#builder()} instead. */
    private Builder() {}

    /**
     * Sets the stage name from which to download the file.
     *
     * <p><b>Examples:</b>
     *
     * <ul>
     *   <li>{@code "@my_stage"} - named internal stage
     *   <li>{@code "@~"} - user stage
     *   <li>{@code "@%table_name"} - table stage
     *   <li>{@code "@external_stage"} - external stage (S3, Azure, GCS)
     * </ul>
     *
     * @param stageName the stage name (must not be null or empty)
     * @return this builder instance
     * @throws IllegalArgumentException if stageName is null or empty
     */
    public Builder setStageName(String stageName) {
      if (stageName == null || stageName.trim().isEmpty()) {
        throw new IllegalArgumentException("stageName cannot be null or empty");
      }
      this.stageName = stageName.trim();
      return this;
    }

    /**
     * Sets the source file name or path within the stage.
     *
     * <p>This can be a simple file name or a path with directories separated by forward slashes.
     *
     * <p><b>Examples:</b>
     *
     * <ul>
     *   <li>{@code "file.csv"} - file in stage root
     *   <li>{@code "data/file.csv"} - file in subdirectory
     *   <li>{@code "2024/01/data.csv.gz"} - file in nested directories
     * </ul>
     *
     * @param sourceFileName the source file name/path (must not be null or empty)
     * @return this builder instance
     * @throws IllegalArgumentException if sourceFileName is null or empty
     */
    public Builder setSourceFileName(String sourceFileName) {
      if (sourceFileName == null || sourceFileName.trim().isEmpty()) {
        throw new IllegalArgumentException("sourceFileName cannot be null or empty");
      }
      this.sourceFileName = sourceFileName.trim();
      return this;
    }

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
     * Builds and validates the {@link DownloadStreamConfig} instance.
     *
     * @return a new {@link DownloadStreamConfig} instance
     * @throws IllegalStateException if required fields (stageName, sourceFileName) are not set
     */
    public DownloadStreamConfig build() {
      if (stageName == null || stageName.isEmpty()) {
        throw new IllegalStateException("stageName is required");
      }
      if (sourceFileName == null || sourceFileName.isEmpty()) {
        throw new IllegalStateException("sourceFileName is required");
      }
      return new DownloadStreamConfig(this);
    }
  }

  @Override
  public String toString() {
    return "DownloadStreamConfig{"
        + "stageName='"
        + stageName
        + '\''
        + ", sourceFileName='"
        + sourceFileName
        + '\''
        + ", decompress="
        + decompress
        + '}';
  }
}
