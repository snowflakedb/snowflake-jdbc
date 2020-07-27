package net.snowflake.client.core;

/** OCSP mode */
public enum OCSPMode {
  /**
   * Fail closed, aka. hard failure mode. The connection is blocked if the revocation status is
   * revoked or it cannot identify the status.
   */
  FAIL_CLOSED(0),

  /**
   * Fail open, aka. soft failure mode. The connection is blocked only if the revocation status is
   * revoked otherwise opened for any reason including the case where the revocation status cannot
   * be retrieved.
   */
  FAIL_OPEN(1),

  /** Insure mode. No OCSP check is made. */
  INSECURE(2);

  private final int value;

  OCSPMode(int value) {
    this.value = value;
  }

  public int getValue() {
    return this.value;
  }
}
