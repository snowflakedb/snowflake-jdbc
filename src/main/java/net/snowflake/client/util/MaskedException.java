package net.snowflake.client.util;

public class MaskedException extends RuntimeException {
  private final Throwable inner;

  public MaskedException(Throwable inner) {
    // Avoid capturing an extra stack trace; we'll copy the inner frames below.
    super(null, null, /* enableSuppression */ true, /* writableStackTrace */ true);
    this.inner = inner;
    if (inner != null) {
      setStackTrace(inner.getStackTrace());
    }
  }

  public Throwable getInner() {
    return inner;
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
    // Mirror Throwable.toString() behavior, but use the inner exception class name so log output
    // remains familiar.
    final String className = inner == null ? getClass().getName() : inner.getClass().getName();
    final String message = getLocalizedMessage();
    final String rendered = (message != null) ? (className + ": " + message) : className;
    return SecretDetector.maskSecrets(rendered);
  }
}
