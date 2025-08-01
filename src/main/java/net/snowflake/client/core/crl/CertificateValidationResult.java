package net.snowflake.client.core.crl;

/** Represents the result of CRL validation for an individual certificate. */
public enum CertificateValidationResult {
  /** Certificate is not revoked according to CRL */
  CERT_UNREVOKED,

  /** Certificate is revoked according to CRL */
  CERT_REVOKED,

  /** Error occurred during CRL validation for this certificate */
  CERT_ERROR
}
