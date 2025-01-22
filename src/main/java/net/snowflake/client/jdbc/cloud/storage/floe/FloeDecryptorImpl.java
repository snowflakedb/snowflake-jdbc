package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import net.snowflake.client.jdbc.cloud.storage.floe.aead.AeadProvider;

class FloeDecryptorImpl extends BaseSegmentProcessor implements FloeDecryptor {
  private final FloeIv floeIv;
  private final AeadProvider aeadProvider;

  FloeDecryptorImpl(
      FloeParameterSpec parameterSpec, FloeKey floeKey, FloeAad floeAad, byte[] floeHeaderAsBytes) {
    super(parameterSpec, floeKey, floeAad);
    byte[] encodedParams = this.parameterSpec.paramEncode();
    if (floeHeaderAsBytes.length
        != encodedParams.length
            + this.parameterSpec.getFloeIvLength().getLength()
            + headerTagLength) {
      throw new IllegalArgumentException("invalid header length");
    }
    ByteBuffer floeHeader = ByteBuffer.wrap(floeHeaderAsBytes);

    byte[] encodedParamsFromHeader = new byte[10];
    floeHeader.get(encodedParamsFromHeader, 0, encodedParamsFromHeader.length);
    if (!Arrays.equals(encodedParams, encodedParamsFromHeader)) {
      throw new IllegalArgumentException("invalid parameters header");
    }

    byte[] floeIvBytes = new byte[this.parameterSpec.getFloeIvLength().getLength()];
    floeHeader.get(floeIvBytes, 0, floeIvBytes.length);
    this.floeIv = new FloeIv(floeIvBytes);
    this.aeadProvider = parameterSpec.getAead().getAeadProvider();

    byte[] headerTagFromHeader = new byte[headerTagLength];
    floeHeader.get(headerTagFromHeader, 0, headerTagFromHeader.length);

    byte[] headerTag =
        keyDerivator.hkdfExpand(
            this.floeKey, floeIv, this.floeAad, HeaderTagFloePurpose.INSTANCE, headerTagLength);
    if (!Arrays.equals(headerTag, headerTagFromHeader)) {
      throw new IllegalArgumentException("invalid header tag");
    }
  }

  private long segmentCounter;

  @Override
  public byte[] processSegment(byte[] input) {
    assertNotClosed();
    ByteBuffer inputBuffer = ByteBuffer.wrap(input);
    try {
      if (isLastSegment(inputBuffer)) {
        return processLastSegment(inputBuffer);
      } else {
        return processNonLastSegment(inputBuffer);
      }
    } catch (Exception e) {
      markAsCompletedExceptionally();
      throw e;
    }
  }

  private boolean isLastSegment(ByteBuffer inputBuffer) {
    int segmentSizeMarker = inputBuffer.getInt();
    try {
      return segmentSizeMarker != NON_TERMINAL_SEGMENT_SIZE_MARKER;
    } finally {
      inputBuffer.rewind();
    }
  }

  private byte[] processNonLastSegment(ByteBuffer inputBuf) {
    try {
      verifyNonLastSegmentLength(inputBuf);
      verifySegmentSizeMarker(inputBuf);
      AeadKey aeadKey = getKey(floeKey, floeIv, floeAad, segmentCounter);
      AeadIv aeadIv = AeadIv.from(inputBuf, parameterSpec.getAead().getIvLength());
      AeadAad aeadAad = AeadAad.nonTerminal(segmentCounter);
      byte[] ciphertext = new byte[inputBuf.remaining()];
      inputBuf.get(ciphertext);
      byte[] decrypted =
          aeadProvider.decrypt(aeadKey.getKey(), aeadIv.getBytes(), aeadAad.getBytes(), ciphertext);
      segmentCounter++;
      return decrypted;
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private void verifyNonLastSegmentLength(ByteBuffer inputBuf) {
    if (inputBuf.capacity() != parameterSpec.getEncryptedSegmentLength()) {
      throw new IllegalArgumentException(
          String.format(
              "segment length mismatch, expected %d, got %d",
              parameterSpec.getEncryptedSegmentLength(), inputBuf.capacity()));
    }
  }

  private void verifySegmentSizeMarker(ByteBuffer inputBuf) {
    int segmentSizeMarker = inputBuf.getInt();
    if (segmentSizeMarker != NON_TERMINAL_SEGMENT_SIZE_MARKER) {
      throw new IllegalArgumentException(
          String.format(
              "segment length marker mismatch, expected: %d, got: %d",
              NON_TERMINAL_SEGMENT_SIZE_MARKER, segmentSizeMarker));
    }
  }

  private byte[] processLastSegment(ByteBuffer inputBuf) {
    verifyLastSegmentLength(inputBuf);
    verifyLastSegmentSizeMarker(inputBuf);
    try {
      AeadKey aeadKey = getKey(floeKey, floeIv, floeAad, segmentCounter);
      AeadIv aeadIv = AeadIv.from(inputBuf, parameterSpec.getAead().getIvLength());
      AeadAad aeadAad = AeadAad.terminal(segmentCounter);
      byte[] ciphertext = new byte[inputBuf.remaining()];
      inputBuf.get(ciphertext);
      byte[] decrypted =
          aeadProvider.decrypt(aeadKey.getKey(), aeadIv.getBytes(), aeadAad.getBytes(), ciphertext);
      closeInternal();
      return decrypted;
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private void verifyLastSegmentLength(ByteBuffer inputBuf) {
    if (inputBuf.capacity()
        < 4 + parameterSpec.getAead().getIvLength() + parameterSpec.getAead().getAuthTagLength()) {
      throw new IllegalArgumentException("last segment is too short");
    }
    if (inputBuf.capacity() > parameterSpec.getEncryptedSegmentLength()) {
      throw new IllegalArgumentException("last segment is too long");
    }
  }

  private void verifyLastSegmentSizeMarker(ByteBuffer inputBuf) {
    int segmentLengthFromSegment = inputBuf.getInt();
    if (segmentLengthFromSegment != inputBuf.capacity()) {
      throw new IllegalArgumentException(
          String.format(
              "last segment length marker mismatch, expected: %d, got: %d",
              inputBuf.capacity(), segmentLengthFromSegment));
    }
  }

  @Override
  public boolean isClosed() {
    return super.isClosed();
  }
}
