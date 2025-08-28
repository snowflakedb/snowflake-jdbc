package net.snowflake.client.core.crl;

class NoopCRLCache implements CRLCache {
  static final CRLCache INSTANCE = new NoopCRLCache();

  @Override
  public CRLCacheEntry get(String crlUrl) {
    return null;
  }

  @Override
  public void put(String crlUrl, CRLCacheEntry entry) {}

  @Override
  public void cleanup() {}
}
