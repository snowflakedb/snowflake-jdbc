package net.snowflake.client.core.auth.oauth;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import com.google.common.html.HtmlEscapers;
import com.nimbusds.oauth2.sdk.id.State;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

class AuthorizationCodeRedirectRequestHandler {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(AuthorizationCodeRedirectRequestHandler.class);

  static String handleRedirectRequest(
      Map<String, String> urlParams,
      CompletableFuture<String> authorizationCodeFuture,
      State expectedState) {
    String response;
    if (urlParams.containsKey("error")) {
      response = "Authorization error: " + urlParams.get("error");
      authorizationCodeFuture.completeExceptionally(
          new SFException(
              ErrorCode.OAUTH_AUTHORIZATION_CODE_FLOW_ERROR,
              String.format(
                  "Error during authorization: %s, %s",
                  urlParams.get("error"), urlParams.get("error_description"))));
    } else if (!expectedState.getValue().equals(urlParams.get("state"))) {
      authorizationCodeFuture.completeExceptionally(
          new SFException(
              ErrorCode.OAUTH_AUTHORIZATION_CODE_FLOW_ERROR,
              String.format(
                  "Invalid authorization request redirection state: %s, expected: %s",
                  urlParams.get("state"), expectedState.getValue())));
      response = "Authorization error: invalid authorization request redirection state";
    } else {
      String authorizationCode = urlParams.get("code");
      if (!isNullOrEmpty(authorizationCode)) {
        logger.debug("Received authorization code on redirect URI");
        response = "Authorization completed successfully.";
        authorizationCodeFuture.complete(authorizationCode);
      } else {
        authorizationCodeFuture.completeExceptionally(
            new SFException(
                ErrorCode.OAUTH_AUTHORIZATION_CODE_FLOW_ERROR,
                String.format(
                    "Authorization code redirect URI server received request without authorization code; queryParams: %s",
                    urlParams)));
        response = "Authorization error: authorization code has not been returned to the driver.";
      }
    }
    return HtmlEscapers.htmlEscaper().escape(response);
  }
}
