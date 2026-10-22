package net.snowflake.client.internal.core;

/**
 * Carries all fields needed to build a v2 token-cache key.
 *
 * <p>Fields are raw (pre-normalization); {@link SecureStorageManager#buildCacheKey} normalizes them
 * before hashing. {@code credentialType} selects the key prefix and the {@code keyData} shape:
 * OAuth types include {@code idp} and {@code role}; MFA and ID tokens do not.
 */
final class CacheKeyInput {
  /** Credential type. The cache-key prefix is {@link CachedCredentialType#getValue()}. */
  final CachedCredentialType credentialType;

  /**
   * Raw IdP token-endpoint URL. Required for OAuth types. Empty for MFA and ID tokens, where the
   * field is absent from {@code keyData}.
   */
  final String idp;

  /** Raw Snowflake server URL or hostname. Must not be null or empty. */
  final String snowflake;

  /** Raw Snowflake username. Must not be null or empty. */
  final String username;

  /** Raw role name. Empty string when no role is configured. Absent from MFA and ID token keys. */
  final String role;

  CacheKeyInput(
      CachedCredentialType credentialType,
      String idp,
      String snowflake,
      String username,
      String role) {
    this.credentialType = credentialType;
    this.idp = idp;
    this.snowflake = snowflake;
    this.username = username;
    this.role = role;
  }
}
