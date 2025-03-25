package net.snowflake.client.core.auth.wif;

import java.util.Map;


class WorkflowIdentityAttestation {

    private final AttestationProviderType provider;
    private final String credential;
    private final Map<String, String> userIdentifiedComponents;

    WorkflowIdentityAttestation(AttestationProviderType provider, String credential, Map<String, String> userIdentifiedComponents) {
        this.provider = provider;
        this.credential = credential;
        this.userIdentifiedComponents = userIdentifiedComponents;
    }
}
