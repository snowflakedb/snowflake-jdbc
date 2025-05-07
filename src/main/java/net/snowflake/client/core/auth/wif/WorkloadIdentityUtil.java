package net.snowflake.client.core.auth.wif;

import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTParser;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpRequestBase;

class WorkloadIdentityUtil {

  private static final SFLogger logger = SFLoggerFactory.getLogger(WorkloadIdentityUtil.class);

  static final String SNOWFLAKE_AUDIENCE_HEADER_NAME = "X-Snowflake-Audience";
  static final String SNOWFLAKE_AUDIENCE = "snowflakecomputing.com";

  // Address commonly used by AWS, Azure & GCP to host instance metadata service
  static final String DEFAULT_METADATA_SERVICE_BASE_URL = "http://169.254.169.254";

  static String performIdentityRequest(HttpRequestBase tokenRequest, SFLoginInput loginInput)
      throws SnowflakeSQLException, IOException {
    return HttpUtil.executeGeneralRequestOmitRequestGuid(
        tokenRequest,
        loginInput.getLoginTimeout(),
        3, // 3s timeout
        loginInput.getSocketTimeoutInMillis(),
        0,
        loginInput.getHttpClientSettingsKey());
  }

  static SubjectAndIssuer extractClaimsWithoutVerifyingSignature(String token) {
    Map<String, Object> claims = extractClaimsMap(token);
    if (claims == null) {
      logger.error("Failed to parse JWT and extract claims");
      return null;
    }
    String issuer = (String) claims.get("iss");
    if (issuer == null) {
      logger.error("Missing issuer claim in JWT token");
      return null;
    }
    String subject = (String) claims.get("sub");
    if (subject == null) {
      logger.error("Missing sub claim in JWT token");
      return null;
    }
    return new SubjectAndIssuer(subject, issuer);
  }

  private static Map<String, Object> extractClaimsMap(String token) {
    try {
      JWT jwt = JWTParser.parse(token);
      return jwt.getJWTClaimsSet().getClaims();
    } catch (Exception e) {
      logger.error("Unable to extract JWT claims from token: {}", e);
      return null;
    }
  }

  static class SubjectAndIssuer {
    private final String subject;
    private final String issuer;

    SubjectAndIssuer(String subject, String issuer) {
      this.issuer = issuer;
      this.subject = subject;
    }

    public String getIssuer() {
      return issuer;
    }

    public String getSubject() {
      return subject;
    }

    public Map<String, String> toMap() {
      Map<String, String> claims = new HashMap<>();
      claims.put("iss", issuer);
      claims.put("sub", subject);
      return claims;
    }
  }
}
