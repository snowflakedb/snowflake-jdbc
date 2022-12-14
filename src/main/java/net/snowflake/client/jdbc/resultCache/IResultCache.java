package net.snowflake.client.jdbc.resultCache;
/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

/** Interface for client side result cache. */
public interface IResultCache {

  /**
   * Add result in the cache. If entry exist, overwrite it.
   *
   * @param queryText Query text.
   * @param queryId Query id.
   * @param sessionId Session id.
   * @param result Result object of type ResultSetSerializable.
   */
  void putResult(String queryText, String queryId, String sessionId, Object result);

  /**
   * Get the result from the cache. If result doesn't exist then return null.
   *
   * @param queryText Query text.
   * @return Result object of type ResultSetSerializable.
   */
  Object getResult(String queryText);

  /**
   * Clear the cache.
   *
   * @param queryText Query text.
   */
  void clean(String queryText);
}
