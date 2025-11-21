package net.snowflake.client.internal.core.crl;

enum CRLValidationResult {
  CHAIN_UNREVOKED,
  CHAIN_REVOKED,
  CHAIN_ERROR
}
