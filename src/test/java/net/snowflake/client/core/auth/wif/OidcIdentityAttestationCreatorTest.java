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
   *     "iss": "https://oidc.eks.us-east-2.amazonaws.com/id/3B869BC5D12CEB5515358621D8085D58",
   *     "iat": 1743692017,
   *     "exp": 1775228014,
   *     "aud": "www.example.com",
   *     "sub": "system:serviceaccount:poc-namespace:oidc-sa"
   * }
   */
  private static final String VALID_TOKEN =
      "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL29pZGMuZWtzLnVzLWVhc3QtMi5hbWF6b25hd3MuY29tL2lkLzNCODY5QkM1RDEyQ0VCNTUxNTM1ODYyMUQ4MDg1RDU4IiwiaWF0IjoxNzQ0Mjg3ODc4LCJleHAiOjE3NzU4MjM4NzgsImF1ZCI6Ind3dy5leGFtcGxlLmNvbSIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDpwb2MtbmFtZXNwYWNlOm9pZGMtc2EifQ.a8H6KRIF1XmM8lkqL6kR8ccInr7wAzQrbKd3ZHFgiEg";

  @Test
  public void shouldReturnProperAttestation() {
    OidcIdentityAttestationCreator attestationCreator =
        new OidcIdentityAttestationCreator(VALID_TOKEN);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();

    Assertions.assertEquals(WorkloadIdentityProviderType.OIDC, attestation.getProvider());
    Assertions.assertEquals(VALID_TOKEN, attestation.getCredential());
    assertEquals(
        "https://oidc.eks.us-east-2.amazonaws.com/id/3B869BC5D12CEB5515358621D8085D58",
        attestation.getUserIdentifierComponents().get("iss"));
    assertEquals(
        "system:serviceaccount:poc-namespace:oidc-sa",
        attestation.getUserIdentifierComponents().get("sub"));
  }

  @Test
  public void missingIssuerScenario() {
    createAttestationAndAssertNull(MISSING_ISSUER_TOKEN);
  }

  @Test
  public void missingSubScenario() {
    createAttestationAndAssertNull(MISSING_SUB_TOKEN);
  }

  @Test
  public void unparsableTokenScenario() {
    createAttestationAndAssertNull(UNPARSABLE_TOKEN);
  }

  private void createAttestationAndAssertNull(String token) {
    OidcIdentityAttestationCreator attestationCreator = new OidcIdentityAttestationCreator(token);
    WorkloadIdentityAttestation attestation = attestationCreator.createAttestation();
    assertNull(attestation);
  }
}
