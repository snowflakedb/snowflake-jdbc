package net.snowflake.client.core;

/**
 * Represents the result of CRL validation for a certificate chain. Based on the CRL validation
 * design specification.
 */
public enum CRLValidationResult {
  /** All certificates in the chain are unrevoked */
  CHAIN_UNREVOKED,

  /** At least one certificate in the chain is revoked */
  CHAIN_REVOKED,

  /** Error occurred during CRL validation (network, parsing, etc.) */
  CHAIN_ERROR
}
