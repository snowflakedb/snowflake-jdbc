package net.snowflake.client.core.auth.wif;

import static net.snowflake.client.core.auth.wif.WorkloadIdentityUtil.DEFAULT_METADATA_SERVICE_BASE_URL;
import static net.snowflake.client.core.auth.wif.WorkloadIdentityUtil.performIdentityRequest;

import java.util.Collections;
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
    gcpMetadataServiceBaseUrl = DEFAULT_METADATA_SERVICE_BASE_URL;
  }

  /** Only for testing purpose */
  GcpIdentityAttestationCreator(SFLoginInput loginInput, String gcpBaseUrl) {
    this.loginInput = loginInput;
    this.gcpMetadataServiceBaseUrl = gcpBaseUrl;
  }

  @Override
  public WorkloadIdentityAttestation createAttestation() {
    logger.debug("Creating GCP identity attestation...");
    String token = fetchTokenFromMetadataService();
    if (token == null) {
      logger.debug("No GCP token was found.");
      return null;
    }
    // if the token has been returned, we can assume that we're on GCP environment
    WorkloadIdentityUtil.SubjectAndIssuer claims =
        WorkloadIdentityUtil.extractClaimsWithoutVerifyingSignature(token);
    if (claims == null) {
      logger.error("Could not extract claims from token");
      return null;
    }

    if (!EXPECTED_GCP_TOKEN_ISSUER.equalsIgnoreCase(claims.getIssuer())) {
      logger.error(
          "Unexpected token issuer: {}, should be {}",
          claims.getIssuer(),
          EXPECTED_GCP_TOKEN_ISSUER);
      return null;
    }

    return new WorkloadIdentityAttestation(
        WorkloadIdentityProviderType.GCP,
        token,
        Collections.singletonMap("sub", claims.getSubject()));
  }

  private String fetchTokenFromMetadataService() {
    String uri =
        gcpMetadataServiceBaseUrl
            + "/computeMetadata/v1/instance/service-accounts/default/identity?audience="
            + WorkloadIdentityUtil.SNOWFLAKE_AUDIENCE;
    HttpGet tokenRequest = new HttpGet(uri);
    tokenRequest.setHeader(METADATA_FLAVOR_HEADER_NAME, METADATA_FLAVOR);
    try {
      return performIdentityRequest(tokenRequest, loginInput);
    } catch (Exception e) {
      logger.debug("GCP metadata server request was not successful: {}" + e);
      return null;
    }
  }
}
