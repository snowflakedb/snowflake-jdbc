package net.snowflake.client.core.auth.wif;

import net.snowflake.client.core.SFException;

interface WorkloadIdentityAttestationCreator {

  WorkloadIdentityAttestation createAttestation() throws SFException;
}
