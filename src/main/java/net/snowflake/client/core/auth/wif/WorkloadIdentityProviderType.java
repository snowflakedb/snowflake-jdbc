package net.snowflake.client.core.auth.wif;

enum WorkloadIdentityProviderType {
  AWS, // Provider that builds an encoded pre-signed GetCallerIdentity request using the current
  // workload's IAM role.
  AZURE, // Provider that requests an OAuth access token for the workload's managed identity.
  GCP, // Provider that requests an ID token for the workload's attached service account.
  OIDC // Provider that looks for an OIDC ID token.
}
