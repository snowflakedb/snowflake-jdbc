package net.snowflake.client.common.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * A class to manage encryption material for remote files for GET and PUT commands.
 *
 * @author ffunke
 */
@SnowflakeJdbcInternalApi
public class RemoteStoreFileEncryptionMaterial {
  private String queryStageMasterKey;
  private String queryId;
  private Long smkId;

  public RemoteStoreFileEncryptionMaterial(String queryStageMasterKey, String queryId, Long smkId) {
    this.queryStageMasterKey = queryStageMasterKey;
    this.queryId = queryId;
    this.smkId = smkId;
  }

  public RemoteStoreFileEncryptionMaterial() {}

  @JsonProperty("queryStageMasterKey")
  public String getQueryStageMasterKey() {
    return queryStageMasterKey;
  }

  public void setQueryStageMasterKey(String queryStageMasterKey) {
    this.queryStageMasterKey = queryStageMasterKey;
  }

  @JsonProperty("queryId")
  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  @JsonProperty("smkId")
  public Long getSmkId() {
    return smkId;
  }

  public void setSmkId(long smkId) {
    this.smkId = smkId;
  }
}
