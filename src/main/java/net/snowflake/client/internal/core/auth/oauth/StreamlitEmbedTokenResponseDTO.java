package net.snowflake.client.internal.core.auth.oauth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response DTO for the Streamlit embed OAuth token-exchange call.
 *
 * <p>The token-exchange response for {@code scope=session:streamlit:&lt;fqn&gt;} does not carry an
 * {@code access_token}; instead it returns a {@code redirect_uri} that embeds a single-use,
 * short-TTL authorization code together with an {@code expires_in} validity window. The existing
 * {@link TokenResponseDTO} requires {@code access_token} and therefore cannot represent this shape,
 * so this dedicated DTO is used instead.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamlitEmbedTokenResponseDTO {

  private final String redirectUri;
  private final long expiresIn;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public StreamlitEmbedTokenResponseDTO(
      @JsonProperty(value = "redirect_uri") String redirectUri,
      @JsonProperty("expires_in") long expiresIn) {
    this.redirectUri = redirectUri;
    this.expiresIn = expiresIn;
  }

  /**
   * The redirect URI carrying the single-use authorization code (as a {@code #code=} fragment or a
   * {@code code=} query parameter). Treated as a secret: never log its value at info level.
   *
   * @return the redirect URI, or null if the server omitted it
   */
  public String getRedirectUri() {
    return redirectUri;
  }

  /**
   * The number of seconds for which the embed URL (authorization code) remains valid.
   *
   * @return the validity window in seconds
   */
  public long getExpiresIn() {
    return expiresIn;
  }
}
