package net.snowflake.client.core.auth.wif;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.jdbc.BaseWiremockTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.AUTHENTICATION)
class GcpIdentityAttestationCreatorLatestIT extends BaseWiremockTest {

  private static final String SCENARIOS_BASE_DIR = MAPPINGS_BASE_DIR + "/wif/gcp";

  /*
   * {
   *     "iss": "https://accounts.google.com",
   *     "iat": 1743692017,
   *     "exp": 1775228014,
   *     "aud": "www.example.com",
   *     "sub": "some-subject"
   * }
   */
  private static final String SUCCESSFUL_FLOW_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/successful_flow.json";

  private static final String SUCCESSFUL_IMPERSONATION_FLOW_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/successful_impersonation_flow.json";

  /*
   * {
   *   "sub": "some-subject",
   *   "iat": 1743761213,
   *   "exp": 1743764813,
   *   "aud": "www.example.com"
   * }
   */
  private static final String MISSING_ISSUER_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/missing_issuer_claim.json";

  /*
   * {
   *   "iss": "https://accounts.google.com",
   *   "iat": 1743761213,
   *   "exp": 1743764813,
   *   "aud": "www.example.com"
   * }
   */
  private static final String MISSING_SUB_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/missing_sub_claim.json";

  // token equal to "unparsable.token"
  private static final String TOKEN_PARSE_ERROR_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/unparsable_token.json";

  // 400 Bad Request
  private static final String HTTP_ERROR_MAPPINGS = SCENARIOS_BASE_DIR + "/http_error.json";

  @Test
  public void successfulFlowScenario() throws SFException {
    importMappingFromResources(SUCCESSFUL_FLOW_SCENARIO_MAPPINGS);
    SFLoginInput loginInput = createLoginInputStub();

    GcpIdentityAttestationCreator attestationCreator =
        new GcpIdentityAttestationCreator(loginInput, getBaseUrl(), getBaseUrl());
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();
    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.GCP, attestation.getProvider());
    assertEquals("some-subject", attestation.getUserIdentifierComponents().get("sub"));
    assertNotNull(attestation.getCredential());
  }

  @Test
  public void missingIssuerScenario() {
    importMappingFromResources(MISSING_ISSUER_SCENARIO_MAPPINGS);
    createAttestationAndAssertExceptionThrown();
  }

  @Test
  public void missingSubScenario() {
    importMappingFromResources(MISSING_SUB_SCENARIO_MAPPINGS);
    createAttestationAndAssertExceptionThrown();
  }

  @Test
  public void unparsableTokenScenario() {
    importMappingFromResources(TOKEN_PARSE_ERROR_SCENARIO_MAPPINGS);
    createAttestationAndAssertExceptionThrown();
  }

  @Test
  public void httpErrorScenario() {
    importMappingFromResources(HTTP_ERROR_MAPPINGS);
    createAttestationAndAssertExceptionThrown();
  }

  @Test
  public void successfulImpersonationFlowScenario() throws SFException {
    importMappingFromResources(SUCCESSFUL_IMPERSONATION_FLOW_SCENARIO_MAPPINGS);
    SFLoginInput loginInput = createLoginInputStub();
    loginInput.setWorkloadIdentityImpersonationPath("delegate1,delegate2,targetServiceAccount");

    GcpIdentityAttestationCreator attestationCreator =
        new GcpIdentityAttestationCreator(loginInput, getBaseUrl(), getBaseUrl());
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();
    assertNotNull(attestation);
    assertEquals(WorkloadIdentityProviderType.GCP, attestation.getProvider());
    assertEquals("some-subject", attestation.getUserIdentifierComponents().get("sub"));
    assertNotNull(attestation.getCredential());
  }

  private void createAttestationAndAssertExceptionThrown() {
    SFLoginInput loginInput = createLoginInputStub();
    GcpIdentityAttestationCreator attestationCreator =
        new GcpIdentityAttestationCreator(loginInput, getBaseUrl(), getBaseUrl());
    assertThrows(SFException.class, attestationCreator::createAttestation);
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
