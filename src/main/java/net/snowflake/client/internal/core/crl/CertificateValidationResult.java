package net.snowflake.client.internal.core.crl;

enum CertificateValidationResult {
  CERT_UNREVOKED,
  CERT_REVOKED,
  CERT_ERROR
}
