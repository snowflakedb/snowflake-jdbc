package net.snowflake.client.core.auth.wif;

import static net.snowflake.client.core.auth.wif.WorkloadIdentityUtil.DEFAULT_METADATA_SERVICE_BASE_URL;
import static net.snowflake.client.core.auth.wif.WorkloadIdentityUtil.SubjectAndIssuer;
import static net.snowflake.client.core.auth.wif.WorkloadIdentityUtil.extractClaimsWithoutVerifyingSignature;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpGet;

@SnowflakeJdbcInternalApi
public class AzureIdentityAttestationCreator implements WorkloadIdentityAttestationCreator {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(AzureIdentityAttestationCreator.class);
  public static final ObjectMapper objectMapper = new ObjectMapper();

  private static final Set<String> ALLOWED_AZURE_TOKEN_ISSUER_PREFIXES =
      new HashSet<>(
          Arrays.asList("https://sts.windows.net/", "https://login.microsoftonline.com/"));
  private static final String DEFAULT_WORKLOAD_IDENTITY_ENTRA_RESOURCE =
      "api://fd3f753b-eed3-462c-b6a7-a4b5bb650aad";

  private final AzureAttestationService azureAttestationService;
  private final SFLoginInput loginInput;
  private final String workloadIdentityEntraResource;
  private final String azureMetadataServiceBaseUrl;

  public AzureIdentityAttestationCreator(
      AzureAttestationService azureAttestationService, SFLoginInput loginInput) {
    this.azureAttestationService = azureAttestationService;
    this.azureMetadataServiceBaseUrl = DEFAULT_METADATA_SERVICE_BASE_URL;
    this.loginInput = loginInput;
    this.workloadIdentityEntraResource = getEntraResource(loginInput);
  }

  /** Only for testing purpose */
  public AzureIdentityAttestationCreator(
      AzureAttestationService azureAttestationService,
      SFLoginInput loginInput,
      String azureMetadataServiceBaseUrl) {
    this.azureAttestationService = azureAttestationService;
    this.azureMetadataServiceBaseUrl = azureMetadataServiceBaseUrl;
    this.loginInput = loginInput;
    this.workloadIdentityEntraResource = getEntraResource(loginInput);
  }

  @Override
  public WorkloadIdentityAttestation createAttestation() {
    logger.debug("Creating Azure identity attestation...");
    String identityEndpoint = azureAttestationService.getIdentityEndpoint();
    HttpGet request;
    if (Strings.isNullOrEmpty(identityEndpoint)) {
      request = createAzureVMIdentityRequest();
    } else {
      String identityHeader = azureAttestationService.getIdentityHeader();
      if (Strings.isNullOrEmpty(identityHeader)) {
        logger.warn("Managed identity is not enabled on this Azure function.");
        return null;
      }
      request =
          createAzureFunctionsIdentityRequest(
              identityEndpoint, identityHeader, azureAttestationService.getClientId());
    }
    String tokenJson = azureAttestationService.fetchTokenFromMetadataService(request, loginInput);
    if (tokenJson == null) {
      logger.debug("Could not fetch Azure token.");
      return null;
    }
    String token = extractTokenFromJson(tokenJson);
    if (token == null) {
      logger.error("No access token found in Azure response.");
      return null;
    }
    SubjectAndIssuer claims = extractClaimsWithoutVerifyingSignature(token);
    if (claims == null) {
      logger.error("Could not extract claims from token");
      return null;
    }
    boolean hasAllowedPrefix =
        ALLOWED_AZURE_TOKEN_ISSUER_PREFIXES.stream()
            .anyMatch(prefix -> claims.getIssuer().startsWith(prefix));
    if (!hasAllowedPrefix) {
      logger.error("Unexpected Azure token issuer: {}", claims.getIssuer());
      return null;
    }
    return new WorkloadIdentityAttestation(
        WorkloadIdentityProviderType.AZURE, token, claims.toMap());
  }

  private String getEntraResource(SFLoginInput loginInput) {
    if (!Strings.isNullOrEmpty(loginInput.getWorkloadIdentityEntraResource())) {
      return loginInput.getWorkloadIdentityEntraResource();
    } else {
      return DEFAULT_WORKLOAD_IDENTITY_ENTRA_RESOURCE;
    }
  }

  private String extractTokenFromJson(String tokenJson) {
    try {
      JsonNode jsonNode = objectMapper.readTree(tokenJson);
      return jsonNode.get("access_token").asText();
    } catch (Exception e) {
      logger.error("Unable to extract token from Azure metadata response: {}", e.getMessage());
      return null;
    }
  }

  private HttpGet createAzureFunctionsIdentityRequest(
      String identityEndpoint, String identityHeader, String managedIdentityClientId) {
    String queryParams = "api-version=2019-08-01&resource=" + workloadIdentityEntraResource;
    if (managedIdentityClientId != null) {
      queryParams += "&client_id=" + managedIdentityClientId;
    }
    HttpGet request = new HttpGet(String.format("%s?%s", identityEndpoint, queryParams));
    request.addHeader("X-IDENTITY-HEADER", identityHeader);
    return request;
  }

  private HttpGet createAzureVMIdentityRequest() {
    String urlWithoutQueryString = azureMetadataServiceBaseUrl + "/metadata/identity/oauth2/token?";
    String queryParams = "api-version=2018-02-01&resource=" + workloadIdentityEntraResource;
    HttpGet request = new HttpGet(urlWithoutQueryString + queryParams);
    request.setHeader("Metadata", "True");
    return request;
  }
}
