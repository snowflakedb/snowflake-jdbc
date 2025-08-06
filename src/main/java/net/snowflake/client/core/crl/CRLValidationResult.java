package net.snowflake.client.core.crl;

enum CRLValidationResult {
  CHAIN_UNREVOKED,
  CHAIN_REVOKED,
  CHAIN_ERROR
}
