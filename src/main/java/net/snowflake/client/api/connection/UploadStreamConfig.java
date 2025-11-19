package net.snowflake.client.api.connection;

import java.io.InputStream;

/**
 * Configuration for uploading data to a Snowflake stage from a stream.
 *
 * <p>This class provides configuration options for the {@link
 * SnowflakeConnection#uploadStream(UploadStreamConfig)} method, which allows uploading data from an
 * input stream to Snowflake internal or external stages.
 *
 * <p><b>Example usage:</b>
 *
 * <pre>{@code
 * try (InputStream dataStream = new FileInputStream("data.csv")) {
 *   UploadStreamConfig config = UploadStreamConfig.builder()
 *       .setStageName("@my_stage")
 *       .setDestFileName("uploaded_data.csv")
 *       .setInputStream(dataStream)
 *       .setCompressData(true)
 *       .build();
 *
 *   connection.uploadStream(config);
 * }
 * }</pre>
 *
 * @see SnowflakeConnection#uploadStream(UploadStreamConfig)
 */
public class UploadStreamConfig {
  private final String stageName;
  private final String destPrefix;
  private final InputStream inputStream;
  private final String destFileName;
  private final boolean compressData;

  /**
   * Private constructor. Use {@link Builder} to create instances.
   *
   * @param builder the builder instance
   */
  private UploadStreamConfig(Builder builder) {
    this.stageName = builder.stageName;
    this.destPrefix = builder.destPrefix;
    this.inputStream = builder.inputStream;
    this.destFileName = builder.destFileName;
    this.compressData = builder.compressData;
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
   * Gets the destination prefix (directory path within the stage).
   *
   * @return the destination prefix, or null if files should be uploaded to stage root
   */
  public String getDestPrefix() {
    return destPrefix;
  }

  /**
   * Gets the input stream containing the data to upload.
   *
   * @return the input stream
   */
  public InputStream getInputStream() {
    return inputStream;
  }

  /**
   * Gets the destination file name.
   *
   * @return the destination file name
   */
  public String getDestFileName() {
    return destFileName;
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
   * <p>This builder provides a fluent API for configuring upload stream operations. All setter
   * methods return the builder instance for method chaining.
   *
   * <p><b>Example:</b>
   *
   * <pre>{@code
   * UploadStreamConfig config = UploadStreamConfig.builder()
   *     .setStageName("@my_stage")
   *     .setDestPrefix("data/2024")
   *     .setDestFileName("file.csv")
   *     .setInputStream(inputStream)
   *     .setCompressData(true)
   *     .build();
   * }</pre>
   */
  public static class Builder {
    private String stageName;
    private String destPrefix;
    private InputStream inputStream;
    private String destFileName;
    private boolean compressData = true;

    /** Private constructor. Use {@link UploadStreamConfig#builder()} instead. */
    private Builder() {}

    /**
     * Sets the stage name to which the file will be uploaded.
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
     * Sets the input stream containing the data to upload.
     *
     * <p><b>Important:</b> The caller is responsible for closing the input stream after the upload
     * completes. Consider using try-with-resources for proper resource management.
     *
     * @param inputStream the input stream (must not be null)
     * @return this builder instance
     * @throws IllegalArgumentException if inputStream is null
     */
    public Builder setInputStream(InputStream inputStream) {
      if (inputStream == null) {
        throw new IllegalArgumentException("inputStream cannot be null");
      }
      this.inputStream = inputStream;
      return this;
    }

    /**
     * Sets the destination file name in the stage.
     *
     * <p>This is the name the file will have after upload. Do not include directory paths here; use
     * {@link #setDestPrefix(String)} for directory paths.
     *
     * <p><b>Examples:</b>
     *
     * <ul>
     *   <li>{@code "data.csv"} - upload as data.csv
     *   <li>{@code "report_2024.txt"} - upload with specific name
     * </ul>
     *
     * @param destFileName the destination file name (must not be null or empty)
     * @return this builder instance
     * @throws IllegalArgumentException if destFileName is null or empty
     */
    public Builder setDestFileName(String destFileName) {
      if (destFileName == null || destFileName.trim().isEmpty()) {
        throw new IllegalArgumentException("destFileName cannot be null or empty");
      }
      this.destFileName = destFileName.trim();
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
     * Builds and validates the {@link UploadStreamConfig} instance.
     *
     * @return a new {@link UploadStreamConfig} instance
     * @throws IllegalStateException if required fields (stageName, inputStream, destFileName) are
     *     not set
     */
    public UploadStreamConfig build() {
      if (stageName == null || stageName.isEmpty()) {
        throw new IllegalStateException("stageName is required");
      }
      if (inputStream == null) {
        throw new IllegalStateException("inputStream is required");
      }
      if (destFileName == null || destFileName.isEmpty()) {
        throw new IllegalStateException("destFileName is required");
      }
      return new UploadStreamConfig(this);
    }
  }

  @Override
  public String toString() {
    return "UploadStreamConfig{"
        + "stageName='"
        + stageName
        + '\''
        + ", destPrefix='"
        + destPrefix
        + '\''
        + ", inputStream="
        + (inputStream != null ? "provided" : "null")
        + ", destFileName='"
        + destFileName
        + '\''
        + ", compressData="
        + compressData
        + '}';
  }
}
