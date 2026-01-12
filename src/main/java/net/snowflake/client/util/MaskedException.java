package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/** Wrapper exception that ensures any secret in log output is masked via {@link SecretDetector}. */
@SnowflakeJdbcInternalApi
public class MaskedException extends RuntimeException {
  private final Throwable inner;

  public MaskedException(Throwable inner) {
    // Avoid capturing an extra stack trace; we'll copy the inner frames below.
    super(null, null, true, true);
    this.inner = inner;
    if (inner != null) {
      setStackTrace(inner.getStackTrace());
    }
  }

  @Override
  public String getMessage() {
    return SecretDetector.maskSecrets(inner == null ? null : inner.getMessage());
  }

  @Override
  public String getLocalizedMessage() {
    return SecretDetector.maskSecrets(inner == null ? null : inner.getLocalizedMessage());
  }

  @Override
  public String toString() {
    return SecretDetector.maskSecrets(inner == null ? null : inner.toString());
  }
}
