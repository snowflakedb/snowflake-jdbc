package net.snowflake.client.jdbc.cloud.storage.floe;

import java.io.Closeable;
import java.io.IOException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

abstract class BaseSegmentProcessor implements Closeable {
  protected static final int NON_TERMINAL_SEGMENT_SIZE_MARKER = -1;
  protected static final int headerTagLength = 32;

  protected final FloeParameterSpec parameterSpec;
  protected final FloeKey floeKey;
  protected final FloeAad floeAad;

  protected final KeyDerivator keyDerivator;

  private AeadKey currentAeadKey;

  private boolean isClosed;
  private boolean completedExceptionally;

  BaseSegmentProcessor(FloeParameterSpec parameterSpec, FloeKey floeKey, FloeAad floeAad) {
    this.parameterSpec = parameterSpec;
    this.floeKey = floeKey;
    this.floeAad = floeAad;
    this.keyDerivator = new KeyDerivator(parameterSpec);
  }

  protected AeadKey getKey(FloeKey floeKey, FloeIv floeIv, FloeAad floeAad, long segmentCounter) {
    if (currentAeadKey == null || segmentCounter % parameterSpec.getKeyRotationModulo() == 0) {
      // we don't need masking, because we derive a new key only when key rotation happens
      currentAeadKey = deriveKey(floeKey, floeIv, floeAad, segmentCounter);
    }
    return currentAeadKey;
  }

  private AeadKey deriveKey(FloeKey floeKey, FloeIv floeIv, FloeAad floeAad, long segmentCounter) {
    byte[] keyBytes =
        keyDerivator.hkdfExpand(
            floeKey,
            floeIv,
            floeAad,
            new DekTagFloePurpose(segmentCounter),
            parameterSpec.getAead().getKeyLength());
    SecretKey key = new SecretKeySpec(keyBytes, parameterSpec.getAead().getJceKeyTypeName());
    return new AeadKey(key);
  }

  protected void closeInternal() {
    isClosed = true;
  }

  protected void markAsCompletedExceptionally() {
    completedExceptionally = true;
  }

  protected void assertNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("stream has already been closed");
    }
  }

  @Override
  public void close() throws IOException {
    if (!isClosed && !completedExceptionally) {
      throw new IllegalStateException("last segment was not processed");
    }
  }

  protected boolean isClosed() {
    return isClosed;
  }
}
