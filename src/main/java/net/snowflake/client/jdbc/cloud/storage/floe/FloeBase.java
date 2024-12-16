package net.snowflake.client.jdbc.cloud.storage.floe;

abstract class FloeBase {
  protected static final int headerTagLength = 32;

  protected final FloeParameterSpec parameterSpec;
  protected final FloeKey floeKey;
  protected final FloeAad floeAad;

  protected final FloeKdf floeKdf;

  FloeBase(FloeParameterSpec parameterSpec, FloeKey floeKey, FloeAad floeAad) {
    this.parameterSpec = parameterSpec;
    this.floeKey = floeKey;
    this.floeAad = floeAad;
    this.floeKdf = new FloeKdf(parameterSpec);
  }
}
