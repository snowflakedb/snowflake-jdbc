package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class FloeDecryptorImpl extends FloeBase implements FloeDecryptor {
  FloeDecryptorImpl(
      FloeParameterSpec parameterSpec, FloeKey floeKey, FloeAad floeAad, byte[] floeHeaderAsBytes) {
    super(parameterSpec, floeKey, floeAad);
    validate(floeHeaderAsBytes);
  }

  public void validate(byte[] floeHeaderAsBytes) {
    byte[] encodedParams = parameterSpec.paramEncode();
    if (floeHeaderAsBytes.length
        != encodedParams.length + parameterSpec.getFloeIvLength().getLength() + headerTagLength) {
      throw new IllegalArgumentException("invalid header length");
    }
    ByteBuffer floeHeader = ByteBuffer.wrap(floeHeaderAsBytes);

    byte[] encodedParamsFromHeader = new byte[10];
    floeHeader.get(encodedParamsFromHeader, 0, encodedParamsFromHeader.length);
    if (!Arrays.equals(encodedParams, encodedParamsFromHeader)) {
      throw new IllegalArgumentException("invalid parameters header");
    }

    byte[] floeIvBytes = new byte[parameterSpec.getFloeIvLength().getLength()];
    floeHeader.get(floeIvBytes, 0, floeIvBytes.length);
    FloeIv floeIv = new FloeIv(floeIvBytes);

    byte[] headerTagFromHeader = new byte[headerTagLength];
    floeHeader.get(headerTagFromHeader, 0, headerTagFromHeader.length);

    byte[] headerTag =
        floeKdf.hkdfExpand(floeKey, floeIv, floeAad, FloePurpose.HEADER_TAG, headerTagLength);
    if (!Arrays.equals(headerTag, headerTagFromHeader)) {
      throw new IllegalArgumentException("invalid header tag");
    }
  }
}
