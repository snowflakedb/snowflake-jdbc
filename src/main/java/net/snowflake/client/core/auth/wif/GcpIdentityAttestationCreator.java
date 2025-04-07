package net.snowflake.client.core.auth.wif;

import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTParser;
import java.util.Collections;
import java.util.Map;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpGet;

@SnowflakeJdbcInternalApi
public class GcpIdentityAttestationCreator implements WorkloadIdentityAttestationCreator {

  private static final String METADATA_FLAVOR_HEADER_NAME = "Metadata-Flavor";
  private static final String METADATA_FLAVOR = "Google";
  private static final String EXPECTED_GCP_TOKEN_ISSUER = "https://accounts.google.com";
  private static final String DEFAULT_GCP_METADATA_SERVICE_BASE_URL = "http://169.254.169.254";

  private final String gcpMetadataServiceBaseUrl;

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(GcpIdentityAttestationCreator.class);

  private final SFLoginInput loginInput;

  public GcpIdentityAttestationCreator(SFLoginInput loginInput) {
    this.loginInput = loginInput;
    gcpMetadataServiceBaseUrl = DEFAULT_GCP_METADATA_SERVICE_BASE_URL;
  }

  /** Only for testing purpose */
  GcpIdentityAttestationCreator(SFLoginInput loginInput, String gcpBaseUrl) {
    this.loginInput = loginInput;
    this.gcpMetadataServiceBaseUrl = gcpBaseUrl;
  }

  @Override
  public WorkloadIdentityAttestation createAttestation() {
    String token = fetchTokenFromMetadataService();
    if (token == null) {
      logger.debug("No GCP token was found.");
      return null;
    }
    Map<String, Object> claims = extractClaims(token);
    if (claims == null) {
      logger.debug("Failed to parse JWT and extract claims");
      return null;
    }
    String issuer = (String) claims.get("iss");
    if (issuer == null) {
      logger.debug("Missing issuer claim in GCP token");
      return null;
    }
    String subject = (String) claims.get("sub");
    if (subject == null) {
      logger.debug("Missing sub claim in GCP token");
      return null;
    }
    if (!issuer.equals(EXPECTED_GCP_TOKEN_ISSUER)) {
      logger.debug("Unexpected GCP token issuer:" + issuer);
      return null;
    }
    return new WorkloadIdentityAttestation(
        WorkloadIdentityProviderType.GCP, token, Collections.singletonMap("sub", subject));
  }

  private Map<String, Object> extractClaims(String token) {
    try {
      JWT jwt = JWTParser.parse(token);
      return jwt.getJWTClaimsSet().getClaims();
    } catch (Exception e) {
      logger.debug("Unable to extract JWT claims from token", e);
      return null;
    }
  }

  private String fetchTokenFromMetadataService() {
    String uri =
        gcpMetadataServiceBaseUrl
            + "/computeMetadata/v1/instance/service-accounts/default/identity?audience="
            + WorkloadIdentityUtil.SNOWFLAKE_AUDIENCE;
    HttpGet tokenRequest = new HttpGet(uri);
    tokenRequest.setHeader(METADATA_FLAVOR_HEADER_NAME, METADATA_FLAVOR);
    try {
      return HttpUtil.executeGeneralRequest(
          tokenRequest,
          loginInput.getLoginTimeout(),
          loginInput.getAuthTimeout(),
          loginInput.getSocketTimeoutInMillis(),
          0,
          loginInput.getHttpClientSettingsKey());
    } catch (Exception e) {
      logger.debug("GCP metadata server request was not successful: " + e.getMessage());
      return null;
    }
  }
}
