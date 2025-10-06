package net.snowflake.client.core.auth.wif;

import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTParser;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpRequestBase;

/**
 * Utility class for Workload Identity Federation (WIF) specific operations. This class contains
 * functions that are used exclusively within the WIF package.
 */
@SnowflakeJdbcInternalApi
public class WorkloadIdentityUtil {

  private static final SFLogger logger = SFLoggerFactory.getLogger(WorkloadIdentityUtil.class);

  // Address commonly used by AWS, Azure & GCP to host instance metadata service
  public static final String DEFAULT_METADATA_SERVICE_BASE_URL = "http://169.254.169.254";

  public static final String SNOWFLAKE_AUDIENCE_HEADER_NAME = "X-Snowflake-Audience";
  public static final String SNOWFLAKE_AUDIENCE = "snowflakecomputing.com";

  /**
   * Performs an HTTP request for WIF identity token retrieval. This method is used by WIF
   * authentication flows to communicate with cloud metadata services.
   */
  public static String performIdentityRequest(HttpRequestBase tokenRequest, SFLoginInput loginInput)
      throws SnowflakeSQLException, IOException {
    return HttpUtil.executeGeneralRequestOmitSnowflakeHeaders(
        tokenRequest,
        loginInput.getLoginTimeout(),
        3, // 3s timeout
        loginInput.getSocketTimeoutInMillis(),
        0,
        loginInput.getHttpClientSettingsKey(),
        null);
  }

  /**
   * Extracts claims (subject and issuer) from a JWT token without verifying the signature. This is
   * used in WIF flows where signature verification is handled elsewhere.
   */
  public static SubjectAndIssuer extractClaimsWithoutVerifyingSignature(String token)
      throws SFException {
    Map<String, Object> claims = extractClaimsMap(token);
    if (claims == null) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "Failed to parse JWT and extract claims");
    }
    String issuer = (String) claims.get("iss");
    if (issuer == null) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "Missing issuer claim in JWT token");
    }
    String subject = (String) claims.get("sub");
    if (subject == null) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "Missing sub claim in JWT token");
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

  /**
   * Container class for JWT subject and issuer claims. Used in WIF authentication flows to pass
   * extracted token claims.
   */
  public static class SubjectAndIssuer {
    private final String subject;
    private final String issuer;

    public SubjectAndIssuer(String subject, String issuer) {
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
