package net.snowflake.client.core.auth.wif;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.jdbc.BaseWiremockTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Tag(TestTags.AUTHENTICATION)
public class AzureIdentityAttestationCreatorLatestIT extends BaseWiremockTest {

  private static final String SCENARIOS_BASE_DIR = MAPPINGS_BASE_DIR + "/wif/azure";

  /*
   * {
   *     "iss": "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
   *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
   * }
   */
  private static final String SUCCESSFUL_FLOW_BASIC_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/successful_flow_basic.json";

  /*
   * {
   *     "iss": "https://login.microsoftonline.com/fa15d692-e9c7-4460-a743-29f29522229/",
   *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
   * }
   */
  private static final String SUCCESSFUL_FLOW_V2_ISSUER_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/successful_flow_v2_issuer.json";

  /*
   * {
   *     "iss": "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
   *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
   * }
   */
  private static final String SUCCESSFUL_FLOW_AZURE_FUNCTIONS_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/successful_flow_azure_functions.json";

  /*
   * {
   *     "iss": "https://login.microsoftonline.com/fa15d692-e9c7-4460-a743-29f29522229/",
   *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
   * }
   */
  private static final String SUCCESSFUL_FLOW_AZURE_FUNCTIONS_V2_ISSUER_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/successful_flow_azure_functions_v2_issuer.json";

  /*
   * {
   *     "iss": "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
   *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
   * }
   */
  private static final String SUCCESSFUL_FLOW_AZURE_FUNCTIONS_NO_CLIENT_ID_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/successful_flow_azure_functions_no_client_id.json";

  /*
   * {
   *     "iss": "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
   *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
   * }
   */
  private static final String
      SUCCESSFUL_FLOW_AZURE_FUNCTIONS_CUSTOM_ENTRA_RESOURCE_SCENARIO_MAPPINGS =
          SCENARIOS_BASE_DIR + "/successful_flow_azure_functions_custom_entra_resource.json";

  /*
   * {
   *     "iss": "https://not.azure.sts.issuer.com",
   *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
   * }
   */
  private static final String INVALID_ISSUER_FLOW_SCENARIO =
      SCENARIOS_BASE_DIR + "/invalid_issuer_flow.json";

  /*
   * {
   *   "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD",
   * }
   */
  private static final String MISSING_ISSUER_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/missing_issuer_claim.json";

  /*
   * {
   *   "iss": "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
   * }
   */
  private static final String MISSING_SUB_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/missing_sub_claim.json";

  // HTTP response that is not in JSON format
  private static final String JSON_PARSE_ERROR_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/non_json_response.json";

  // token equal to "unparsable.token"
  private static final String TOKEN_PARSE_ERROR_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/unparsable_token.json";

  // 400 Bad Request
  private static final String HTTP_ERROR_MAPPINGS = SCENARIOS_BASE_DIR + "/http_error.json";

  @Test
  public void successfulFlowBasicScenario() {
    importMappingFromResources(SUCCESSFUL_FLOW_BASIC_SCENARIO_MAPPINGS);
    SFLoginInput loginInput = createLoginInputStub();
    AzureAttestationService attestationServiceMock = createAttestationServiceSpyForBasicFLow();
    executeAndAssertCorrectAttestation(attestationServiceMock, loginInput);
  }

  @Test
  public void successfulFlowV2IssuerScenario() {
    importMappingFromResources(SUCCESSFUL_FLOW_V2_ISSUER_SCENARIO_MAPPINGS);
    SFLoginInput loginInput = createLoginInputStub();
    AzureAttestationService attestationServiceMock = createAttestationServiceSpyForBasicFLow();
    executeAndAssertCorrectAttestationWithIssuer(
        attestationServiceMock,
        loginInput,
        "https://login.microsoftonline.com/fa15d692-e9c7-4460-a743-29f29522229/");
  }

  @Test
  public void successfulFlowAzureFunctionsScenario() {
    importMappingFromResources(SUCCESSFUL_FLOW_AZURE_FUNCTIONS_SCENARIO_MAPPINGS);
    SFLoginInput loginInput = createLoginInputStub();
    AzureAttestationService attestationServiceSpy = Mockito.spy(AzureAttestationService.class);
    Mockito.when(attestationServiceSpy.getIdentityEndpoint())
        .thenReturn(getBaseUrl() + "metadata/identity/endpoint/from/env");
    Mockito.when(attestationServiceSpy.getIdentityHeader())
        .thenReturn("some-identity-header-from-env");
    Mockito.when(attestationServiceSpy.getClientId()).thenReturn("managed-client-id-from-env");

    executeAndAssertCorrectAttestation(attestationServiceSpy, loginInput);
  }

  @Test
  public void successfulFlowAzureFunctionsWithV2IssuerScenario() {
    importMappingFromResources(SUCCESSFUL_FLOW_AZURE_FUNCTIONS_V2_ISSUER_SCENARIO_MAPPINGS);
    SFLoginInput loginInput = createLoginInputStub();
    AzureAttestationService attestationServiceSpy = Mockito.spy(AzureAttestationService.class);
    Mockito.when(attestationServiceSpy.getIdentityEndpoint())
        .thenReturn(getBaseUrl() + "metadata/identity/endpoint/from/env");
    Mockito.when(attestationServiceSpy.getIdentityHeader())
        .thenReturn("some-identity-header-from-env");
    Mockito.when(attestationServiceSpy.getClientId()).thenReturn("managed-client-id-from-env");

    executeAndAssertCorrectAttestationWithIssuer(
        attestationServiceSpy,
        loginInput,
        "https://login.microsoftonline.com/fa15d692-e9c7-4460-a743-29f29522229/");
  }

  @Test
  public void successfulFlowAzureFunctionsNoClientIdScenario() {
    importMappingFromResources(SUCCESSFUL_FLOW_AZURE_FUNCTIONS_NO_CLIENT_ID_SCENARIO_MAPPINGS);
    SFLoginInput loginInput = createLoginInputStub();
    AzureAttestationService attestationServiceSpy = Mockito.spy(AzureAttestationService.class);
    Mockito.when(attestationServiceSpy.getIdentityEndpoint())
        .thenReturn(getBaseUrl() + "metadata/identity/endpoint/from/env");
    Mockito.when(attestationServiceSpy.getIdentityHeader())
        .thenReturn("some-identity-header-from-env");
    Mockito.when(attestationServiceSpy.getClientId()).thenReturn(null);

    executeAndAssertCorrectAttestation(attestationServiceSpy, loginInput);
  }

  @Test
  public void successfulFlowAzureFunctionsCustomEntraResourceScenario() {
    importMappingFromResources(
        SUCCESSFUL_FLOW_AZURE_FUNCTIONS_CUSTOM_ENTRA_RESOURCE_SCENARIO_MAPPINGS);
    SFLoginInput loginInput = createLoginInputStub();
    loginInput.setWorkloadIdentityEntraResource("api://1111111-2222-3333-44444-55555555");
    AzureAttestationService attestationServiceSpy = Mockito.spy(AzureAttestationService.class);
    Mockito.when(attestationServiceSpy.getIdentityEndpoint())
        .thenReturn(getBaseUrl() + "metadata/identity/endpoint/from/env");
    Mockito.when(attestationServiceSpy.getIdentityHeader())
        .thenReturn("some-identity-header-from-env");
    Mockito.when(attestationServiceSpy.getClientId()).thenReturn("managed-client-id-from-env");

    executeAndAssertCorrectAttestation(attestationServiceSpy, loginInput);
  }

  @Test
  public void azureFunctionsFlowErrorNoIdentityHeader() {
    SFLoginInput loginInput = createLoginInputStub();
    AzureAttestationService attestationServiceMock = Mockito.mock(AzureAttestationService.class);
    Mockito.when(attestationServiceMock.getIdentityEndpoint())
        .thenReturn(getBaseUrl() + "metadata/identity/endpoint/from/env");
    Mockito.when(attestationServiceMock.getIdentityHeader()).thenReturn(null);
    Mockito.when(attestationServiceMock.getClientId()).thenReturn(null);

    executeAndAssertNullAttestation(attestationServiceMock, loginInput);
  }

  @Test
  public void basicFlowErrorInvalidIssuer() {
    executeErrorScenarioAndAssertNullAttestation(INVALID_ISSUER_FLOW_SCENARIO);
  }

  @Test
  public void basicFlowErrorMissingIssuer() {
    executeErrorScenarioAndAssertNullAttestation(MISSING_ISSUER_SCENARIO_MAPPINGS);
  }

  @Test
  public void basicFlowErrorMissingSub() {
    executeErrorScenarioAndAssertNullAttestation(MISSING_SUB_SCENARIO_MAPPINGS);
  }

  @Test
  public void basicFlowErrorUnparsableToken() {
    executeErrorScenarioAndAssertNullAttestation(TOKEN_PARSE_ERROR_SCENARIO_MAPPINGS);
  }

  @Test
  public void basicFlowUnparsableJsonError() {
    executeErrorScenarioAndAssertNullAttestation(JSON_PARSE_ERROR_SCENARIO_MAPPINGS);
  }

  @Test
  public void basicFlowHttpError() {
    executeErrorScenarioAndAssertNullAttestation(HTTP_ERROR_MAPPINGS);
  }

  private void executeErrorScenarioAndAssertNullAttestation(
      String tokenParseErrorScenarioMappings) {
    importMappingFromResources(tokenParseErrorScenarioMappings);
    SFLoginInput loginInput = createLoginInputStub();
    AzureAttestationService attestationServiceSpy = createAttestationServiceSpyForBasicFLow();
    executeAndAssertNullAttestation(attestationServiceSpy, loginInput);
  }

  private static AzureAttestationService createAttestationServiceSpyForBasicFLow() {
    AzureAttestationService attestationServiceMock = Mockito.spy(AzureAttestationService.class);
    Mockito.when(attestationServiceMock.getIdentityEndpoint()).thenReturn(null);
    Mockito.when(attestationServiceMock.getIdentityHeader()).thenReturn(null);
    Mockito.when(attestationServiceMock.getClientId()).thenReturn(null);
    return attestationServiceMock;
  }

  private void executeAndAssertCorrectAttestation(
      AzureAttestationService attestationServiceMock, SFLoginInput loginInput) {
    executeAndAssertCorrectAttestationWithIssuer(
        attestationServiceMock,
        loginInput,
        "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/");
  }

  private void executeAndAssertCorrectAttestationWithIssuer(
      AzureAttestationService attestationServiceMock,
      SFLoginInput loginInput,
      String expectedIssuer) {
    AzureIdentityAttestationCreator attestationCreator =
        new AzureIdentityAttestationCreator(attestationServiceMock, loginInput, getBaseUrl());

    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();
    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.AZURE, attestation.getProvider());
    assertEquals(
        "77213E30-E8CB-4595-B1B6-5F050E8308FD",
        attestation.getUserIdentifierComponents().get("sub"));
    assertEquals(expectedIssuer, attestation.getUserIdentifierComponents().get("iss"));
    assertNotNull(attestation.getCredential());
  }

  private void executeAndAssertNullAttestation(
      AzureAttestationService attestationServiceMock, SFLoginInput loginInput) {
    AzureIdentityAttestationCreator attestationCreator =
        new AzureIdentityAttestationCreator(attestationServiceMock, loginInput, getBaseUrl());

    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();
    assertNull(attestation);
  }

  private String getBaseUrl() {
    return String.format("http://%s:%d/", WIREMOCK_HOST, wiremockHttpPort);
  }

  private SFLoginInput createLoginInputStub() {
    SFLoginInput loginInputStub = new SFLoginInput();
    loginInputStub.setSocketTimeout(Duration.ofMinutes(5));
    loginInputStub.setHttpClientSettingsKey(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN));
    return loginInputStub;
  }
}
