/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.util.HashMap;
import java.util.TreeSet;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Most Recently Used and Priority based cache. A separate cache for each connection in the driver.
 */
public class QueryContextCache {
  private int capacity; // Capacity of the cache
  private HashMap<Long, QueryContextElement> idMap; // Map for id and QCC

  private TreeSet<QueryContextElement> treeSet; // Order data as per priority

  private HashMap<Long, QueryContextElement> priorityMap; // Map for priority and QCC

  static final SFLogger logger = SFLoggerFactory.getLogger(QueryContextCache.class);

  /**
   * Constructor.
   *
   * @param capacity Maximum capacity of the cache.
   */
  public QueryContextCache(int capacity) {
    this.capacity = capacity;
    idMap = new HashMap<>();
    priorityMap = new HashMap<>();
    treeSet = new TreeSet<>();
  }

  /**
   * Merge a new element comes from the server with the existing cache. Merge is based on read time
   * stamp for the same id and based on priority for two different ids.
   *
   * @param id Database id.
   * @param readTimestamp Last time read metadata from FDB.
   * @param priority 0..N number, where 0 is highest priority. Eviction policy is based on priority.
   * @param context Opaque query context.
   */
  public void merge(long id, long readTimestamp, long priority, byte[] context) {
    if (idMap.containsKey(id)) {
      // ID found in the cache
      QueryContextElement qcc = idMap.get(id);
      if (readTimestamp > qcc.readTimestamp) {
        if (qcc.priority == priority) {
          // Same priority, overwrite new data at same place
          qcc.readTimestamp = readTimestamp;
          qcc.context = context;
        } else {
          // Change in priority
          QueryContextElement newQCC =
              new QueryContextElement(id, readTimestamp, priority, context);

          replaceQCC(qcc, newQCC);
        } // new priority
      } // new data is recent
      else if (readTimestamp == qcc.readTimestamp && qcc.priority != priority) {
        // Same read timestamp but change in priority
        QueryContextElement newQCC = new QueryContextElement(id, readTimestamp, priority, context);
        replaceQCC(qcc, newQCC);
      }
    } // id found
    else {
      // new id
      if (priorityMap.containsKey(priority)) {
        // Same priority with different id
        QueryContextElement qcc = priorityMap.get(priority);
        // Replace with new data
        qcc.id = id;
        qcc.readTimestamp = readTimestamp;
        qcc.context = context;
      } else {
        // new priority
        // Add new element in the cache
        QueryContextElement newQCC = new QueryContextElement(id, readTimestamp, priority, context);
        addQCC(newQCC);
      }
    }
  }

  /**
   * After the merge, loop through priority list and make sure cache is at most capacity. Remove all
   * other elements from the list based on priority.
   */
  public void checkCacheCapacity() {

    logger.debug(
        "checkCacheCapacity() called. treeSet size {} cache capacity {}", treeSet.size(), capacity);

    if (treeSet.size() > capacity) {
      // remove elements based on priority
      while (treeSet.size() > capacity) {
        QueryContextElement qcc = treeSet.last();
        removeQCC(qcc);
      }
    }

    logger.debug(
        "checkCacheCapacity() returns. treeSet size {} cache capacity {}",
        treeSet.size(),
        capacity);
  }

  /** Clear the cache. */
  public void clearCache() {
    logger.debug("clearCache() called");

    idMap.clear();
    priorityMap.clear();
    treeSet.clear();
  }

  /**
   * Add an element in the cache.
   *
   * @param qcc element to add
   */
  private void addQCC(QueryContextElement qcc) {
    idMap.put(qcc.id, qcc);
    priorityMap.put(qcc.priority, qcc);
    treeSet.add(qcc);
  }

  /**
   * Remove an element from the cache.
   *
   * @param qcc element to remove.
   */
  private void removeQCC(QueryContextElement qcc) {
    treeSet.remove(qcc);
    priorityMap.remove(qcc.priority);
    idMap.remove(qcc.id);
  }

  /**
   * Replace the cache element with a new response element. Remove old element exist in the cache
   * and add a new element received.
   *
   * @param oldQCC an element exist in the cache
   * @param newQCC a new element just received.
   */
  private void replaceQCC(QueryContextElement oldQCC, QueryContextElement newQCC) {
    // Remove old element from the cache
    removeQCC(oldQCC);

    // Add new element in the cache
    addQCC(newQCC);
  }

  /**
   * Get all elements in the cache in the order of the priority.
   *
   * @return TreeSet containing cache elements
   */
  TreeSet<QueryContextElement> getElements() {
    return treeSet;
  }

  /** Debugging purpose, log the all entries in the cache. */
  void logCacheEntries() {
    if (logger.isDebugEnabled()) {
      TreeSet<QueryContextElement> elements = getElements();
      for (final QueryContextElement elem : elements) {
        logger.debug(
            " Cache Entry: id: {} readTimestamp: {} priority: {} ",
            elem.id,
            elem.readTimestamp,
            elem.priority);
      }
    }
  }
}
