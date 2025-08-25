package net.snowflake.client.core.auth.wif;

import net.snowflake.client.core.SFException;

interface WorkloadIdentityAttestationCreator {

  /**
   * @return corresponding attestation or null if it couldn't be loaded
   */
  WorkloadIdentityAttestation createAttestation() throws SFException;
}
