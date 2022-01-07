/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.io.InputStream;
import java.util.Properties;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFSession;

/**
 * This class manages the parameters to call SnowflakeFileTransferAgent.uploadWithoutConnection()
 */
public class SnowflakeFileTransferConfig {
  private SnowflakeFileTransferMetadata metadata;
  private InputStream uploadStream;
  private boolean requireCompress;
  private int networkTimeoutInMilli;
  private OCSPMode ocspMode;
  private Properties proxyProperties;
  private String prefix;
  private String destFileName;
  private SFSession session; // Optional, added for S3 and Azure
  private String command; // Optional, added for S3 and Azure
  private boolean useS3RegionalUrl; // only for S3 us-east-1 private link deployments
  private String streamingIngestClientName;
  private String streamingIngestClientKey;

  public SnowflakeFileTransferConfig(Builder builder) {
    this.metadata = builder.metadata;
    this.uploadStream = builder.uploadStream;
    this.requireCompress = builder.requireCompress;
    this.networkTimeoutInMilli = builder.networkTimeoutInMilli;
    this.ocspMode = builder.ocspMode;
    this.proxyProperties = builder.proxyProperties;
    this.prefix = builder.prefix;
    this.destFileName = builder.destFileName;
    this.session = builder.session;
    this.command = builder.command;
    this.useS3RegionalUrl = builder.useS3RegionalUrl;
    this.streamingIngestClientKey = builder.streamingIngestClientKey;
    this.streamingIngestClientName = builder.streamingIngestClientName;
  }

  public SnowflakeFileTransferMetadata getSnowflakeFileTransferMetadata() {
    return metadata;
  }

  public InputStream getUploadStream() {
    return uploadStream;
  }

  public boolean getRequireCompress() {
    return requireCompress;
  }

  public int getNetworkTimeoutInMilli() {
    return networkTimeoutInMilli;
  }

  public OCSPMode getOcspMode() {
    return ocspMode;
  }

  public Properties getProxyProperties() {
    return proxyProperties;
  }

  public String getPrefix() {
    return prefix;
  }

  public String getDestFileName() {
    return destFileName;
  }

  public SFSession getSession() {
    return session;
  }

  public String getCommand() {
    return command;
  }

  public boolean getUseS3RegionalUrl() {
    return useS3RegionalUrl;
  }

  public String getStreamingIngestClientName() {
    return this.streamingIngestClientName;
  }

  public String getStreamingIngestClientKey() {
    return this.streamingIngestClientKey;
  }

  // Builder class
  public static class Builder {
    private SnowflakeFileTransferMetadata metadata = null;
    private InputStream uploadStream = null;
    private boolean requireCompress = true;
    private int networkTimeoutInMilli = 0;
    private OCSPMode ocspMode = null;
    private Properties proxyProperties = null;
    private String prefix = null;
    private String destFileName = null;
    private SFSession session = null;
    private String command = null;
    private boolean useS3RegionalUrl = false; // only for S3 us-east-1 private link deployments
    private String streamingIngestClientName;
    private String streamingIngestClientKey;

    public static Builder newInstance() {
      return new Builder();
    }

    private Builder() {}

    // Build method to deal with outer class
    // to return outer instance
    public SnowflakeFileTransferConfig build() throws IllegalArgumentException {
      // Validate required parameters
      if (metadata == null) {
        throw new IllegalArgumentException("Snowflake File Transfer metadata is needed.");
      }
      if (uploadStream == null) {
        throw new IllegalArgumentException("Upload data stream is needed.");
      }
      if (ocspMode == null) {
        throw new IllegalArgumentException("Upload OCSP mode is needed.");
      }

      // Create the object
      return new SnowflakeFileTransferConfig(this);
    }

    public Builder setSnowflakeFileTransferMetadata(SnowflakeFileTransferMetadata metadata) {
      this.metadata = metadata;
      return this;
    }

    public Builder setUploadStream(InputStream uploadStream) {
      this.uploadStream = uploadStream;
      return this;
    }

    public Builder setRequireCompress(boolean requireCompress) {
      this.requireCompress = requireCompress;
      return this;
    }

    public Builder setNetworkTimeoutInMilli(int networkTimeoutInMilli) {
      this.networkTimeoutInMilli = networkTimeoutInMilli;
      return this;
    }

    public Builder setOcspMode(OCSPMode ocspMode) {
      this.ocspMode = ocspMode;
      return this;
    }

    public Builder setProxyProperties(Properties proxyProperties) {
      this.proxyProperties = proxyProperties;
      return this;
    }

    public Builder setPrefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder setDestFileName(String destFileName) {
      this.destFileName = destFileName;
      return this;
    }

    public Builder setSFSession(SFSession session) {
      this.session = session;
      return this;
    }

    public Builder setCommand(String command) {
      this.command = command;
      return this;
    }

    public Builder setUseS3RegionalUrl(boolean useS3RegUrl) {
      this.useS3RegionalUrl = useS3RegUrl;
      return this;
    }

    /** Streaming ingest client name, used to calculate streaming ingest billing per client */
    public Builder setStreamingIngestClientName(String streamingIngestClientName) {
      this.streamingIngestClientName = streamingIngestClientName;
      return this;
    }

    /**
     * Streaming ingest client key provided by Snowflake, used to calculate streaming ingest billing
     * per client
     */
    public Builder setStreamingIngestClientKey(String streamingIngestClientKey) {
      this.streamingIngestClientKey = streamingIngestClientKey;
      return this;
    }
  }
}
