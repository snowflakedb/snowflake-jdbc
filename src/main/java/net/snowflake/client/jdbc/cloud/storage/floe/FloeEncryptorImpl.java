package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;

class FloeEncryptorImpl extends BaseSegmentProcessor implements FloeEncryptor {

  private final FloeIv floeIv;
  private AeadKey currentAeadKey;

  private long segmentCounter;

  private final byte[] header;

  FloeEncryptorImpl(FloeParameterSpec parameterSpec, FloeKey floeKey, FloeAad floeAad) {
    super(parameterSpec, floeKey, floeAad);
    this.floeIv =
        FloeIv.generateRandom(parameterSpec.getFloeRandom(), parameterSpec.getFloeIvLength());
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
    try {
      AeadKey aeadKey = getKey(floeKey, floeIv, floeAad, segmentCounter);
      AeadIv aeadIv =
          AeadIv.generateRandom(
              parameterSpec.getFloeRandom(), parameterSpec.getAead().getIvLength());
      AeadAad aeadAad = AeadAad.nonTerminal(segmentCounter++);
      AeadProvider aeadProvider = parameterSpec.getAead().getAeadProvider();
      // it works as long as AEAD returns auth tag as a part of the ciphertext
      byte[] ciphertextWithAuthTag =
          aeadProvider.encrypt(aeadKey.getKey(), aeadIv.getBytes(), aeadAad.getBytes(), input);
      return segmentToBytes(aeadIv, ciphertextWithAuthTag);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private byte[] segmentToBytes(AeadIv aeadIv, byte[] ciphertextWithAuthTag) {
    ByteBuffer output = ByteBuffer.allocate(parameterSpec.getEncryptedSegmentLength());
    output.putInt(NON_TERMINAL_SEGMENT_SIZE_MARKER);
    output.put(aeadIv.getBytes());
    output.put(ciphertextWithAuthTag);
    return output.array();
  }

  private void verifySegmentLength(byte[] input) {
    if (input.length != parameterSpec.getPlainTextSegmentLength()) {
      throw new IllegalArgumentException(
          String.format(
              "segment length mismatch, expected %d, got %d",
              parameterSpec.getPlainTextSegmentLength(), input.length));
    }
  }
}