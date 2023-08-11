package net.snowflake.client.core;

import java.util.Map;

/**
 * Base class for SF HTTP input types. Currently contains only the identical properties between
 * them, as the properties have slightly different spellings between the current inputs.
 */
abstract class SFInputBase<T extends SFInputBase<T>> {

  Map<String, String> additionalHttpHeaders;

  Map<String, String> getAdditionalHttpHeaders() {
    return additionalHttpHeaders;
  }

  /**
   * Set additional http headers to apply to the outgoing request. The additional headers cannot be
   * used to replace or overwrite a header in use by the driver. These will be applied to the
   * outgoing request. Primarily used by Snowsight, as described in {@link
   * HttpUtil#applyAdditionalHeaders(org.apache.http.client.methods.HttpRequestBase, Map)}
   *
   * @param additionalHttpHeaders The new headers to add
   * @return The input object, for chaining
   * @see HttpUtil#applyAdditionalHeaders(org.apache.http.client.methods.HttpRequestBase, Map)
   */
  @SuppressWarnings("unchecked")
  public T setAdditionalHttpHeaders(Map<String, String> additionalHttpHeaders) {
    this.additionalHttpHeaders = additionalHttpHeaders;
    return (T) this;
  }
}
