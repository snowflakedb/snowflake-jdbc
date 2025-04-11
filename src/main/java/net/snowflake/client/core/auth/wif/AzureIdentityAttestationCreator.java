package net.snowflake.client.core.auth.wif;

import static net.snowflake.client.core.auth.wif.WorkloadIdentityUtil.DEFAULT_METADATA_SERVICE_BASE_URL;
import static net.snowflake.client.core.auth.wif.WorkloadIdentityUtil.SubjectAndIssuer;
import static net.snowflake.client.core.auth.wif.WorkloadIdentityUtil.extractClaimsWithoutVerifyingSignature;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;

@SnowflakeJdbcInternalApi
public class AzureIdentityAttestationCreator implements WorkloadIdentityAttestationCreator {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(AzureIdentityAttestationCreator.class);
  public static final ObjectMapper objectMapper = new ObjectMapper();

  private static final String EXPECTED_GCP_TOKEN_ISSUER_PREFIX = "https://sts.windows.net/";

  private final AzureAttestationService azureAttestationService;
  private final SFLoginInput loginInput;
  private final String azureMetadataServiceBaseUrl;

  public AzureIdentityAttestationCreator(
      AzureAttestationService azureAttestationService, SFLoginInput loginInput) {
    this.azureAttestationService = azureAttestationService;
    this.azureMetadataServiceBaseUrl = DEFAULT_METADATA_SERVICE_BASE_URL;
    this.loginInput = loginInput;
  }

  /** Only for testing purpose */
  public AzureIdentityAttestationCreator(
      AzureAttestationService azureAttestationService,
      SFLoginInput loginInput,
      String azureMetadataServiceBaseUrl) {
    this.azureAttestationService = azureAttestationService;
    this.azureMetadataServiceBaseUrl = azureMetadataServiceBaseUrl;
    this.loginInput = loginInput;
  }

  @Override
  public WorkloadIdentityAttestation createAttestation() {
    String identityEndpoint = azureAttestationService.getIdentityEndpoint();
    HttpGet request;
    if (Strings.isNullOrEmpty(identityEndpoint)) {
      request = createBasicAzureIdentityRequest();
    } else {
      String identityHeader = azureAttestationService.getIdentityHeader();
      if (Strings.isNullOrEmpty(identityHeader)) {
        logger.warn("Managed identity is not enabled on this Azure function.");
        return null;
      }
      request = createAzureFunctionsIdentityRequest(identityEndpoint, identityHeader);
    }
    String tokenJson = fetchTokenFromMetadataService(request);
    if (tokenJson == null) {
      logger.debug("Could not fetch Azure token.");
      return null;
    }
    String token = extractTokenFromJson(tokenJson);
    if (token == null) {
      logger.debug("No access token found in Azure response.");
      return null;
    }
    SubjectAndIssuer claims = extractClaimsWithoutVerifyingSignature(token);
    if (claims == null) {
      return null;
    }
    if (!claims.getIssuer().startsWith(EXPECTED_GCP_TOKEN_ISSUER_PREFIX)) {
      logger.debug("Unexpected Azure token issuer: {}", claims.getIssuer());
      return null;
    }
    return new WorkloadIdentityAttestation(
        WorkloadIdentityProviderType.AZURE, token, claims.toMap());
  }

  private String extractTokenFromJson(String tokenJson) {
    try {
      JsonNode jsonNode = objectMapper.readTree(tokenJson);
      return jsonNode.get("access_token").asText();
    } catch (Exception e) {
      logger.debug("Unable to extract token from Azure metadata response: {}", e.getMessage());
      return null;
    }
  }

  private String fetchTokenFromMetadataService(HttpRequestBase tokenRequest) {
    try {
      return WorkloadIdentityUtil.performIdentityRequest(tokenRequest, loginInput);
    } catch (Exception e) {
      logger.debug("Azure metadata server request was not successful.");
      return null;
    }
  }

  private HttpGet createAzureFunctionsIdentityRequest(
      String identityEndpoint, String identityHeader) {
    String queryParams =
        "api-version=2019-08-01&resource=" + loginInput.getSnowflakeEntraResource();
    String managedIdentityClientId = azureAttestationService.getClientId();
    if (managedIdentityClientId != null) {
      queryParams += "&client-id=" + managedIdentityClientId;
    }
    HttpGet request = new HttpGet(String.format("%s?%s", identityEndpoint, queryParams));
    request.addHeader("X-IDENTITY-HEADER", identityHeader);
    return request;
  }

  private HttpGet createBasicAzureIdentityRequest() {
    String urlWithoutQueryString = azureMetadataServiceBaseUrl + "/metadata/identity/oauth2/token?";
    String queryParams =
        "api-version=2018-02-01&resource=" + loginInput.getSnowflakeEntraResource();
    HttpGet request = new HttpGet(urlWithoutQueryString + queryParams);
    request.setHeader("Metadata", "True");
    return request;
  }
}
