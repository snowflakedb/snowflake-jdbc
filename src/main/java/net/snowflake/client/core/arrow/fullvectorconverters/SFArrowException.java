package net.snowflake.client.core.arrow.fullvectorconverters;

public class SFArrowException extends Exception {
  private final ArrowErrorCode errorCode;

  public SFArrowException(ArrowErrorCode errorCode, String message) {
    this(errorCode, message, null);
  }

  public SFArrowException(ArrowErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public ArrowErrorCode getErrorCode() {
    return errorCode;
  }

  @Override
  public String toString() {
    return super.toString() + (getErrorCode() != null ? ", errorCode = " + getErrorCode() : "");
  }
}
