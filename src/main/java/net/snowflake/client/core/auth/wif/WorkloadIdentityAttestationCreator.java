package net.snowflake.client.core.auth.wif;

interface WorkloadIdentityAttestationCreator {

  /**
   * @return corresponding attestation or null if it couldn't be loaded
   */
  WorkloadIdentityAttestation createAttestation();
}
