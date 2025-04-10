package net.snowflake.client.core.auth.wif;

interface WorkloadIdentityAttestationCreator {

  /**
   * @return according attestation or null if failed
   */
  WorkloadIdentityAttestation createAttestation();
}
