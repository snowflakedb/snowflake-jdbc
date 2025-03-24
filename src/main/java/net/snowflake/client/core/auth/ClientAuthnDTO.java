package net.snowflake.client.core.auth;

import java.util.Map;
import javax.annotation.Nullable;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class ClientAuthnDTO {

  // contains all the required data for current authn step
  private final Map<String, Object> data;

  /*
   * current state
   * tokenized string with all current parameters and the authn step
   */
  private final String inFlightCtx;

  public ClientAuthnDTO(Map<String, Object> data, @Nullable String inFlightCtx) {
    this.data = data;
    this.inFlightCtx = inFlightCtx;
  }

  /** Required by Jackson */
  public Map<String, Object> getData() {
    return data;
  }

  /** Required by Jackson */
  public String getInFlightCtx() {
    return inFlightCtx;
  }
}
