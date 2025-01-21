package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;

class FloeEncryptorImpl extends BaseSegmentProcessor implements FloeEncryptor {
  private final FloeIv floeIv;
  private final AeadProvider aeadProvider;
  private AeadKey currentAeadKey;

  private long segmentCounter;

  private final byte[] header;

  FloeEncryptorImpl(FloeParameterSpec parameterSpec, FloeKey floeKey, FloeAad floeAad) {
    super(parameterSpec, floeKey, floeAad);
    this.floeIv =
        FloeIv.generateRandom(parameterSpec.getFloeRandom(), parameterSpec.getFloeIvLength());
    this.aeadProvider = parameterSpec.getAead().getAeadProvider();
    this.header = buildHeader();
  }

  private byte[] buildHeader() {
    byte[] parametersEncoded = parameterSpec.paramEncode();
    byte[] floeIvBytes = floeIv.getBytes();
    byte[] headerTag =
        keyDerivator.hkdfExpand(
            floeKey, floeIv, floeAad, HeaderTagFloePurpose.INSTANCE, headerTagLength);

    ByteBuffer result =
        ByteBuffer.allocate(parametersEncoded.length + floeIvBytes.length + headerTag.length);
    result.put(parametersEncoded);
    result.put(floeIvBytes);
    result.put(headerTag);
    if (result.hasRemaining()) {
      throw new IllegalArgumentException("Header is too long");
    }
    return result.array();
  }

  @Override
  public byte[] getHeader() {
    return header;
  }

  @Override
  public byte[] processSegment(byte[] input) {
    verifySegmentLength(input);
    // TODO assert State.Counter != 2^40-1 # Prevent overflow
    verifyMaxSegmentNumberNotReached();
    try {
      AeadKey aeadKey = getKey(floeKey, floeIv, floeAad, segmentCounter);
      AeadIv aeadIv =
          AeadIv.generateRandom(
              parameterSpec.getFloeRandom(), parameterSpec.getAead().getIvLength());
      AeadAad aeadAad = AeadAad.nonTerminal(segmentCounter);
      // it works as long as AEAD returns auth tag as a part of the ciphertext
      byte[] ciphertextWithAuthTag =
          aeadProvider.encrypt(aeadKey.getKey(), aeadIv.getBytes(), aeadAad.getBytes(), input);
      byte[] encoded = segmentToBytes(aeadIv, ciphertextWithAuthTag);
      segmentCounter++;
      return encoded;
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private void verifySegmentLength(byte[] input) {
    if (input.length != parameterSpec.getPlainTextSegmentLength()) {
      throw new IllegalArgumentException(
          String.format(
              "segment length mismatch, expected %d, got %d",
              parameterSpec.getPlainTextSegmentLength(), input.length));
    }
  }

  private void verifyMaxSegmentNumberNotReached() {
    if (segmentCounter >= parameterSpec.getMaxSegmentNumber() - 1) {
      throw new IllegalStateException("maximum segment number reached");
    }
  }

  private byte[] segmentToBytes(AeadIv aeadIv, byte[] ciphertextWithAuthTag) {
    ByteBuffer output = ByteBuffer.allocate(parameterSpec.getEncryptedSegmentLength());
    output.putInt(NON_TERMINAL_SEGMENT_SIZE_MARKER);
    output.put(aeadIv.getBytes());
    output.put(ciphertextWithAuthTag);
    return output.array();
  }

  @Override
  public byte[] processLastSegment(byte[] input) {
    verifyLastSegmentNotEmpty(input);
    try {
      AeadKey aeadKey = getKey(floeKey, floeIv, floeAad, segmentCounter);
      AeadIv aeadIv =
          AeadIv.generateRandom(
              parameterSpec.getFloeRandom(), parameterSpec.getAead().getIvLength());
      AeadAad aeadAad = AeadAad.terminal(segmentCounter);
      byte[] ciphertextWithAuthTag =
          aeadProvider.encrypt(aeadKey.getKey(), aeadIv.getBytes(), aeadAad.getBytes(), input);
      return lastSegmentToBytes(aeadIv, ciphertextWithAuthTag);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private byte[] lastSegmentToBytes(AeadIv aeadIv, byte[] ciphertextWithAuthTag) {
    int lastSegmentLength = 4 + aeadIv.getBytes().length + ciphertextWithAuthTag.length;
    ByteBuffer output = ByteBuffer.allocate(lastSegmentLength);
    output.putInt(lastSegmentLength);
    output.put(aeadIv.getBytes());
    output.put(ciphertextWithAuthTag);
    return output.array();
  }

  private void verifyLastSegmentNotEmpty(byte[] input) {
    // TODO
//    if (input.length == 0) {
//      throw new IllegalArgumentException("last segment is empty");
//    }
    if (input.length > parameterSpec.getPlainTextSegmentLength()) {
      throw new IllegalArgumentException(String.format("last segment is too long, got %d, max is %d", input.length, parameterSpec.getPlainTextSegmentLength()));
    }
  }
}
