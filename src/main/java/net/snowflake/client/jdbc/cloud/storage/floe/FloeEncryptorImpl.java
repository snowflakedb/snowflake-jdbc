package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;

class FloeEncryptorImpl extends FloeBase implements FloeEncryptor {
  private final FloeIv floeIv;

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
        floeKdf.hkdfExpand(floeKey, floeIv, floeAad, FloePurpose.HEADER_TAG, headerTagLength);

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
}
