package net.snowflake.client.core.crl;

interface CRLCache {
  CRLCacheEntry get(String crlUrl);

  void put(String crlUrl, CRLCacheEntry entry);

  void cleanup();
}
