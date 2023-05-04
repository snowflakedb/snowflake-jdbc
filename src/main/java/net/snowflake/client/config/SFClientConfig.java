package net.snowflake.client.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** POJO class for Snowflake's client config. */
public class SFClientConfig {
  @JsonProperty("common")
  private CommonProps commonProps;

  public SFClientConfig() {}

  public SFClientConfig(CommonProps commonProps) {
    this.commonProps = commonProps;
  }

  public CommonProps getCommonProps() {
    return commonProps;
  }

  public void setCommonProps(CommonProps commonProps) {
    this.commonProps = commonProps;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SFClientConfig that = (SFClientConfig) o;
    return Objects.equals(commonProps, that.commonProps);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commonProps);
  }

  public static class CommonProps {
    @JsonProperty("log_level")
    private String logLevel;

    @JsonProperty("log_path")
    private String logPath;

    public CommonProps() {}

    public void CommonProps(String logLevel, String logPath) {
      this.logLevel = logLevel;
      this.logPath = logPath;
    }

    public String getLogLevel() {
      return logLevel;
    }

    public void setLogLevel(String logLevel) {
      this.logLevel = logLevel;
    }

    public String getLogPath() {
      return logPath;
    }

    public void setLogPath(String logPath) {
      this.logPath = logPath;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CommonProps that = (CommonProps) o;
      return Objects.equals(logLevel, that.logLevel) && Objects.equals(logPath, that.logPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(logLevel, logPath);
    }
  }
}
