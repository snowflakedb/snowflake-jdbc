package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Arrays;

public class FloeDecryptorImpl extends BaseSegmentProcessor implements FloeDecryptor {
  private final FloeIv floeIv;
  private long segmentCounter;

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

    byte[] headerTagFromHeader = new byte[headerTagLength];
    floeHeader.get(headerTagFromHeader, 0, headerTagFromHeader.length);

    byte[] headerTag =
        keyDerivator.hkdfExpand(
            this.floeKey, floeIv, this.floeAad, HeaderTagFloePurpose.INSTANCE, headerTagLength);
    if (!Arrays.equals(headerTag, headerTagFromHeader)) {
      throw new IllegalArgumentException("invalid header tag");
    }
  }

  @Override
  public byte[] processSegment(byte[] input) {
    try {
      verifySegmentLength(input);
      ByteBuffer inputBuf = ByteBuffer.wrap(input);
      verifySegmentSizeMarker(inputBuf);
      AeadKey aeadKey = getKey(floeKey, floeIv, floeAad, segmentCounter);
      AeadIv aeadIv = AeadIv.from(inputBuf, parameterSpec.getAead().getIvLength());
      AeadAad aeadAad = AeadAad.nonTerminal(segmentCounter++);
      AeadProvider aeadProvider = parameterSpec.getAead().getAeadProvider();
      byte[] ciphertext = new byte[inputBuf.remaining()];
      inputBuf.get(ciphertext);
      return aeadProvider.decrypt(
          aeadKey.getKey(), aeadIv.getBytes(), aeadAad.getBytes(), ciphertext);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private void verifySegmentLength(byte[] input) {
    if (input.length != parameterSpec.getEncryptedSegmentLength()) {
      throw new IllegalArgumentException(
          String.format(
              "segment length mismatch, expected %d, got %d",
              parameterSpec.getEncryptedSegmentLength(), input.length));
    }
  }

  private void verifySegmentSizeMarker(ByteBuffer inputBuf) {
    int segmentSizeMarker = inputBuf.getInt();
    if (segmentSizeMarker != NON_TERMINAL_SEGMENT_SIZE_MARKER) {
      throw new IllegalStateException(
          String.format(
              "segment length marker mismatch, expected: %d, got :%d",
              NON_TERMINAL_SEGMENT_SIZE_MARKER, segmentSizeMarker));
    }
  }
}
