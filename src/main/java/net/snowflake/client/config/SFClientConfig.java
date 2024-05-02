package net.snowflake.client.config;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** POJO class for Snowflake's client config. */
public class SFClientConfig {
  @JsonProperty("common")
  private CommonProps commonProps;

  @JsonIgnore private String configFilePath;

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

  public String getConfigFilePath() {
    return configFilePath;
  }

  public void setConfigFilePath(String configFilePath) {
    this.configFilePath = configFilePath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
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

    @JsonAnySetter private Map<String, Object> unknownKeys = new LinkedHashMap<>();

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

    Map<String, Object> getUnknownKeys() {
      return unknownKeys;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CommonProps that = (CommonProps) o;
      return Objects.equals(logLevel, that.logLevel) && Objects.equals(logPath, that.logPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(logLevel, logPath);
    }
  }
}
