package net.snowflake.client.core;

/**
 * Represents the result of CRL validation for an individual certificate. Based on the CRL
 * validation design specification.
 */
public enum CertificateValidationResult {
  /** Certificate is not revoked according to CRL */
  CERT_UNREVOKED,

  /** Certificate is revoked according to CRL */
  CERT_REVOKED,

  /** Error occurred during CRL validation for this certificate */
  CERT_ERROR
}
