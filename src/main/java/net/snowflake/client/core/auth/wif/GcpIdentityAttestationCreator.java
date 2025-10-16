package net.snowflake.client.core.auth.wif;

import static net.snowflake.client.core.auth.wif.WorkloadIdentityUtil.DEFAULT_METADATA_SERVICE_BASE_URL;
import static net.snowflake.client.core.auth.wif.WorkloadIdentityUtil.performIdentityRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

@SnowflakeJdbcInternalApi
public class GcpIdentityAttestationCreator implements WorkloadIdentityAttestationCreator {

  private static final String METADATA_FLAVOR_HEADER_NAME = "Metadata-Flavor";
  private static final String METADATA_FLAVOR = "Google";
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String DEFAULT_GCP_IAM_CREDENTIALS_URL =
      "https://iamcredentials.googleapis.com";

  private final String gcpMetadataServiceBaseUrl;
  private final String gcpIamCredentialsBaseUrl;

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(GcpIdentityAttestationCreator.class);

  private final SFLoginInput loginInput;

  public GcpIdentityAttestationCreator(SFLoginInput loginInput) {
    this.loginInput = loginInput;
    this.gcpMetadataServiceBaseUrl = DEFAULT_METADATA_SERVICE_BASE_URL;
    this.gcpIamCredentialsBaseUrl = DEFAULT_GCP_IAM_CREDENTIALS_URL;
  }

  /** Only for testing purpose */
  GcpIdentityAttestationCreator(SFLoginInput loginInput, String gcpBaseUrl, String gcpIamUrl) {
    this.loginInput = loginInput;
    this.gcpMetadataServiceBaseUrl = gcpBaseUrl;
    gcpIamCredentialsBaseUrl = gcpIamUrl;
  }

  @Override
  public WorkloadIdentityAttestation createAttestation() throws SFException {
    String token;

    if (loginInput.getWorkloadIdentityImpersonationPath().isEmpty()) {
      logger.debug("Creating GCP identity attestation...");
      token = getGcpIdentityTokenFromMetadataService();
    } else {
      logger.debug("Creating GCP identity attestation with impersonation...");
      token = getGcpIdentityTokenViaImpersonation();
    }

    if (token == null) {
      throw new SFException(ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "No GCP token was found.");
    }
    // if the token has been returned, we can assume that we're on GCP environment
    WorkloadIdentityUtil.SubjectAndIssuer claims =
        WorkloadIdentityUtil.extractClaimsWithoutVerifyingSignature(token);

    return new WorkloadIdentityAttestation(
        WorkloadIdentityProviderType.GCP,
        token,
        Collections.singletonMap("sub", claims.getSubject()));
  }

  private String getGcpIdentityTokenFromMetadataService() throws SFException {
    String uri =
        gcpMetadataServiceBaseUrl
            + "/computeMetadata/v1/instance/service-accounts/default/identity?audience="
            + WorkloadIdentityUtil.SNOWFLAKE_AUDIENCE;
    return executeGcpTokenRequest(uri);
  }

  private String fetchTokenFromMetadataService() throws SFException {
    String uri =
        gcpMetadataServiceBaseUrl + "/computeMetadata/v1/instance/service-accounts/default/token";
    String response = executeGcpTokenRequest(uri);
    try {
      JsonNode responseBody = objectMapper.readTree(response);
      return responseBody.get("access_token").asText();
    } catch (JsonProcessingException e) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "GCP metadata server request for token was not successful: " + e.getMessage());
    }
  }

  private String executeGcpTokenRequest(String uri) throws SFException {
    HttpGet tokenRequest = new HttpGet(uri);
    tokenRequest.setHeader(METADATA_FLAVOR_HEADER_NAME, METADATA_FLAVOR);
    try {
      return performIdentityRequest(tokenRequest, loginInput);
    } catch (Exception e) {
      logger.error("GCP metadata server request was not successful", e);
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "GCP metadata server request was not successful: " + e.getMessage());
    }
  }

  private String getGcpIdentityTokenViaImpersonation() throws SFException {
    String accessToken = fetchTokenFromMetadataService();

    List<String> fullServiceAccountPaths =
        loginInput.getWorkloadIdentityImpersonationPath().stream()
            .map(sa -> "projects/-/serviceAccounts/" + sa)
            .collect(Collectors.toList());

    String targetServiceAccount = fullServiceAccountPaths.get(fullServiceAccountPaths.size() - 1);
    List<String> delegates = fullServiceAccountPaths.subList(0, fullServiceAccountPaths.size() - 1);

    String url =
        String.format(gcpIamCredentialsBaseUrl + "/v1/%s:generateIdToken", targetServiceAccount);

    HttpPost request = new HttpPost(url);
    request.setHeader("Authorization", "Bearer " + accessToken);
    request.setHeader("Content-Type", "application/json");

    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("delegates", delegates);
    requestBody.put("audience", WorkloadIdentityUtil.SNOWFLAKE_AUDIENCE);

    try {
      String json = objectMapper.writeValueAsString(requestBody);
      request.setEntity(new StringEntity(json, StandardCharsets.UTF_8));

      String response = performIdentityRequest(request, loginInput);
      JsonNode responseBody = objectMapper.readTree(response);
      return responseBody.get("token").asText();
    } catch (Exception e) {
      logger.error("Error fetching GCP impersonated identity token", e);
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "Error fetching GCP impersonated identity token: " + e.getMessage());
    }
  }
}
