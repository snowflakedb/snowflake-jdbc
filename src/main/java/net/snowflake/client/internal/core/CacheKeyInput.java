package net.snowflake.client.internal.core;

/**
 * Carries all fields needed to build a v2 token-cache key.
 *
 * <p>Fields are raw (pre-normalization); {@link SecureStorageManager#buildCacheKey} normalizes them
 * before hashing.
 */
final class CacheKeyInput {
  /** PascalCase cache-key token-type string, e.g. {@code "MfaToken"}, {@code "IdToken"}. */
  final String tokenType;

  /** Raw IdP / token-endpoint URL. Empty string when IdP == Snowflake server. */
  final String idp;

  /** Raw Snowflake server URL or hostname. Must not be null or empty. */
  final String snowflake;

  /** Raw Snowflake username. Must not be null or empty. */
  final String username;

  /** Raw role name. Empty string for MFA flows. */
  final String role;

  CacheKeyInput(String tokenType, String idp, String snowflake, String username, String role) {
    this.tokenType = tokenType;
    this.idp = idp;
    this.snowflake = snowflake;
    this.username = username;
    this.role = role;
  }
}
