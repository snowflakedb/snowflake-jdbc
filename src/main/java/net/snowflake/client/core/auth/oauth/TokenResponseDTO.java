package net.snowflake.client.core.auth.oauth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
@JsonIgnoreProperties(ignoreUnknown = true)
public class TokenResponseDTO {

  private final String accessToken;
  private final String refreshToken;
  private final String tokenType;
  private final String scope;
  private final String username;
  private final boolean idpInitiated;
  private final long expiresIn;
  private final long refreshTokenExpiresIn;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public TokenResponseDTO(
      @JsonProperty(value = "access_token", required = true) String accessToken,
      @JsonProperty("refresh_token") String refreshToken,
      @JsonProperty("token_type") String tokenType,
      @JsonProperty("scope") String scope,
      @JsonProperty("username") String username,
      @JsonProperty("idp_initiated") boolean idpInitiated,
      @JsonProperty("expires_in") long expiresIn,
      @JsonProperty("refresh_token_expires_in") long refreshTokenExpiresIn) {
    this.accessToken = accessToken;
    this.tokenType = tokenType;
    this.refreshToken = refreshToken;
    this.scope = scope;
    this.username = username;
    this.idpInitiated = idpInitiated;
    this.expiresIn = expiresIn;
    this.refreshTokenExpiresIn = refreshTokenExpiresIn;
  }

  public String getAccessToken() {
    return accessToken;
  }

  public String getTokenType() {
    return tokenType;
  }

  public String getRefreshToken() {
    return refreshToken;
  }

  public String getScope() {
    return scope;
  }

  public long getExpiresIn() {
    return expiresIn;
  }

  public String getUsername() {
    return username;
  }

  public long getRefreshTokenExpiresIn() {
    return refreshTokenExpiresIn;
  }

  public boolean isIdpInitiated() {
    return idpInitiated;
  }
}
