package net.snowflake.client.core.auth.wif;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OidcIdentityAttestationCreatorTest {

  private static final String UNPARSABLE_TOKEN = "unparsable_token";

  /*
   * {
   *   "sub": "some-subject",
   *   "iat": 1743761213,
   *   "exp": 1743764813,
   *   "aud": "www.example.com"
   * }
   */
  private static final String MISSING_ISSUER_TOKEN =
      "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJFUzI1NiIsImtpZCI6ImU2M2I5NzA1OTRiY2NmZTAxMDlkOTg4OWM2MDk3OWEwIn0.eyJzdWIiOiJzb21lLXN1YmplY3QiLCJpYXQiOjE3NDM3NjEyMTMsImV4cCI6MTc0Mzc2NDgxMywiYXVkIjoid3d3LmV4YW1wbGUuY29tIn0.H6sN6kjA82EuijFcv-yCJTqau5qvVTCsk0ZQ4gvFQMkB7c71XPs4lkwTa7ZlNNlx9e6TpN1CVGnpCIRDDAZaDw";

  /*
   * {
   *   "iss": "https://accounts.google.com",
   *   "iat": 1743761213,
   *   "exp": 1743764813,
   *   "aud": "www.example.com"
   * }
   */
  private static final String MISSING_SUB_TOKEN =
      "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJFUzI1NiIsImtpZCI6ImU2M2I5NzA1OTRiY2NmZTAxMDlkOTg4OWM2MDk3OWEwIn0.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJpYXQiOjE3NDM3NjEyMTMsImV4cCI6MTc0Mzc2NDgxMywiYXVkIjoid3d3LmV4YW1wbGUuY29tIn0.w0njdpfWFETVK8Ktq9GdvuKRQJjvhOplcSyvQ_zHHwBUSMapqO1bjEWBx5VhGkdECZIGS1VY7db_IOqT45yOMA";

  /*
   * {
   *     "iss": "https://accounts.google.com",
   *     "iat": 1743692017,
   *     "exp": 1775228014,
   *     "aud": "www.example.com",
   *     "sub": "some-subject"
   * }
   */
  private static final String VALID_TOKEN =
      "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJpYXQiOjE3NDM2OTIwMTcsImV4cCI6MTc3NTIyODAxNCwiYXVkIjoid3d3LmV4YW1wbGUuY29tIiwic3ViIjoic29tZS1zdWJqZWN0In0.k7018udXQjw-sgVY8sTLTnNrnJoGwVpjE6HozZN-h0w";

  @Test
  public void shouldReturnProperAttestation() {
    OidcIdentityAttestationCreator attestationCreator =
        new OidcIdentityAttestationCreator(VALID_TOKEN);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    Assertions.assertEquals(WorkloadIdentityProviderType.OIDC, attestation.getProvider());
    Assertions.assertEquals(VALID_TOKEN, attestation.getCredential());
    assertEquals("some-subject", attestation.getUserIdentifiedComponents().get("sub"));
    assertEquals(
        "https://accounts.google.com", attestation.getUserIdentifiedComponents().get("iss"));
  }

  @Test
  public void missingIssuerScenario() {
    creatAttestationAndAssertNull(MISSING_ISSUER_TOKEN);
  }

  @Test
  public void missingSubScenario() {
    creatAttestationAndAssertNull(MISSING_SUB_TOKEN);
  }

  @Test
  public void unparsableTokenScenario() {
    creatAttestationAndAssertNull(UNPARSABLE_TOKEN);
  }

  private void creatAttestationAndAssertNull(String token) {
    OidcIdentityAttestationCreator attestationCreator = new OidcIdentityAttestationCreator(token);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();
    assertNull(attestation);
  }
}
