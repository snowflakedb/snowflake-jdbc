package net.snowflake.client.jdbc.cloud.storage.floe;

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import javax.crypto.Mac;

class KeyDerivator {
  private final FloeParameterSpec parameterSpec;

  KeyDerivator(FloeParameterSpec parameterSpec) {
    this.parameterSpec = parameterSpec;
  }

  byte[] hkdfExpand(
      FloeKey floeKey, FloeIv floeIv, FloeAad floeAad, FloePurpose purpose, int length) {
    byte[] encodedParams = parameterSpec.paramEncode();
    byte[] purposeBytes = purpose.generate();
    ByteBuffer info =
        ByteBuffer.allocate(
            encodedParams.length
                + floeIv.getBytes().length
                + purposeBytes.length
                + floeAad.getBytes().length);
    info.put(encodedParams);
    info.put(floeIv.getBytes());
    info.put(purposeBytes);
    info.put(floeAad.getBytes());
    return hkdfExpandInternal(parameterSpec.getHash(), floeKey, info.array(), length);
  }

  private byte[] hkdfExpandInternal(Hash hash, FloeKey prk, byte[] info, int len) {
    try {
      Mac mac = Mac.getInstance(hash.getJceName());
      mac.init(prk.getKey());
      mac.update(info);
      mac.update((byte) 1);
      byte[] bytes = mac.doFinal();
      if (bytes.length != len) {
        return Arrays.copyOf(bytes, len);
      }
      return bytes;
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new RuntimeException(e);
    }
  }
}
