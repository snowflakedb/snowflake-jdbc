package net.snowflake.client.jdbc.cloud.storage.floe;

import javax.crypto.SecretKey;

public class Floe {
  private final FloeParameterSpec parameterSpec;

  private Floe(FloeParameterSpec parameterSpec) {
    this.parameterSpec = parameterSpec;
  }

  public static Floe getInstance(FloeParameterSpec parameterSpec) {
    return new Floe(parameterSpec);
  }

  public FloeEncryptor createEncryptor(SecretKey key, byte[] aad) {
    return new FloeEncryptorImpl(parameterSpec, new FloeKey(key), new FloeAad(aad));
  }

  public FloeDecryptor createDecryptor(SecretKey key, byte[] aad, byte[] floeHeader) {
    return new FloeDecryptorImpl(parameterSpec, new FloeKey(key), new FloeAad(aad), floeHeader);
  }
}
