package net.snowflake.client.core.auth.wif;

import net.snowflake.client.core.SFException;

interface WorkflowIdentityAttestationProvider {

    WorkflowIdentityAttestation createAttestation() throws SFException;
}
