/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;
import static net.snowflake.client.jdbc.SnowflakeDriver.implementVersion;
import net.snowflake.client.jdbc.SnowflakeUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Comparator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Most Recently Used and Priority based cache. A separate cache for each connection in the driver.
 */
public class QueryContextCache {
  private final int capacity; // Capacity of the cache
  private final HashMap<Long, QueryContextElement> idMap; // Map for id and QCC

  private final TreeSet<QueryContextElement> treeSet; // Order data as per priority

  private final HashMap<Long, QueryContextElement> priorityMap; // Map for priority and QCC

  private static final SFLogger logger = SFLoggerFactory.getLogger(QueryContextCache.class);

  private static ObjectMapper jsonObjectMapper;

  static{
    jsonObjectMapper = new ObjectMapper();
  }

  /**
   * Constructor.
   *
   * @param capacity Maximum capacity of the cache.
   */
  public QueryContextCache(int capacity) {
    this.capacity = capacity;
    idMap = new HashMap<>();
    priorityMap = new HashMap<>();
    treeSet = new TreeSet<>(Comparator
    .comparingLong(QueryContextElement::getPriority)
    .thenComparingLong(QueryContextElement::getId)
    .thenComparingLong(QueryContextElement::getReadTimestamp));
  }

  /**
   * Merge a new element comes from the server with the existing cache. Merge is based on read time
   * stamp for the same id and based on priority for two different ids.
   *
   * @param id Database id.
   * @param readTimestamp Last time read metadata from FDB.
   * @param priority 0 to N number, where 0 is the highest priority. Eviction policy is based on
   *     priority.
   * @param context Opaque query context.
   */
  void merge(long id, long readTimestamp, long priority, byte[] context) {
    if (idMap.containsKey(id)) {
      // ID found in the cache
      QueryContextElement qce = idMap.get(id);
      if (readTimestamp > qce.readTimestamp) {
        if (qce.priority == priority) {
          // Same priority, overwrite new data at same place
          qce.readTimestamp = readTimestamp;
          qce.context = context;
        } else {
          // Change in priority
          QueryContextElement newQCE =
              new QueryContextElement(id, readTimestamp, priority, context);

          replaceQCE(qce, newQCE);
        } // new priority
      } // new data is recent
      else if (readTimestamp == qce.readTimestamp && qce.priority != priority) {
        // Same read timestamp but change in priority
        QueryContextElement newQCE = new QueryContextElement(id, readTimestamp, priority, context);
        replaceQCE(qce, newQCE);
      }
    } // id found
    else {
      // new id
      if (priorityMap.containsKey(priority)) {

        // Same priority with different id
        QueryContextElement qce = priorityMap.get(priority);
        // Replace with new data
        QueryContextElement newQCE = new QueryContextElement(id, readTimestamp, priority, context);
        replaceQCE(qce, newQCE);
      } else {
        // new priority
        // Add new element in the cache
        QueryContextElement newQCE = new QueryContextElement(id, readTimestamp, priority, context);
        addQCE(newQCE);
      }
    }
  }

  /**
   * After the merge, loop through priority list and make sure cache is at most capacity. Remove all
   * other elements from the list based on priority.
   */
  void checkCacheCapacity() {
    logger.debug(
        "checkCacheCapacity() called. treeSet size {} cache capacity {}", treeSet.size(), capacity);

    if (treeSet.size() > capacity) {
      // remove elements based on priority
      while (treeSet.size() > capacity) {
        QueryContextElement qce = treeSet.last();
        removeQCE(qce);
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
  * @param data: the QueryContext Object serialized as a JSON format string
  */
  public void deserializeQueryContextJson(String data) {

    synchronized (this) {
      // Log existing cache entries
      logCacheEntries();

      if (data == null || data.length() == 0) {
        // Clear the cache
        clearCache();

        // Log existing cache entries
        logCacheEntries();

        return;
      }

    try{
      JsonNode rootNode = jsonObjectMapper.readTree(data);

      // Deserialize the main entry. An example JSON is:
      // {
      //   "main_entry": {
      //     "id": 1,
      //     "read_timestamp": 123456789,
      //     "priority": 0,
      //     "context": "base64 encoded context"
      // }
      JsonNode mainEntryNode = rootNode.path("main_entry");
      if (!mainEntryNode.isMissingNode()) {
        QueryContextElement mainEntry = deserializeQueryContextElement(mainEntryNode);
        merge(mainEntry.id, mainEntry.readTimestamp, mainEntry.priority, mainEntry.context);
      }

      // Deserialize the entries. An example JSON is:
      // {
      //   "entries": [
      //     {
      //       "id": 2,
      //       "read_timestamp": 123456789,
      //       "priority": 1,
      //       "context": "base64 encoded context"
      //     },
      //     {
      //       "id": 3,
      //       "read_timestamp": 123456789,
      //       "priority": 2,
      //       "context": "base64 encoded context"
      //     }
      //   ]

      JsonNode entriesNode = rootNode.path("entries");
      if (entriesNode != null && entriesNode.isArray()) {
          for (JsonNode entryNode : entriesNode) {
            QueryContextElement entry = deserializeQueryContextElement(entryNode);
            merge(entry.id, entry.readTimestamp, entry.priority, entry.context);
          }
      }
    }catch (Exception e) {
      logger.debug("deserializeQueryContextJson: Exception = {}", e.getMessage());
      // Not rethrowing. clear the cache as incomplete merge can lead to unexpected behavior.
      clearCache();
    }

    // After merging all entries, truncate to capacity
    checkCacheCapacity();

    // Log existing cache entries
    logCacheEntries();

  }// Synchronized
}

private static QueryContextElement deserializeQueryContextElement(JsonNode node) throws IOException {
  QueryContextElement entry = new QueryContextElement();
    JsonNode idNode = node.path("id");
    if (idNode.isNumber()) {
        entry.setId(idNode.asLong());
    }

    JsonNode timestampNode = node.path("timestamp");
    if (timestampNode.isNumber()) {
        entry.setReadTimestamp(timestampNode.asLong());
    }

    JsonNode priorityNode = node.path("priority");
    if (priorityNode.isNumber()) {
        entry.setPriority(priorityNode.asLong());
    }

    JsonNode contextNode = node.path("context");
    if (contextNode.isTextual()) {
        byte[] contextBytes = contextNode.asText().getBytes();
        entry.setContext(contextBytes);
    }

    return entry;
}

/**
 * Serialize the current QueryContext into a JSON format string
*/
public String serializeQueryContextJson() {
  synchronized (this) {
    // Log existing cache entries
    logCacheEntries();

    TreeSet<QueryContextElement> elements = getElements();
    if (elements.size() == 0) return null;

    try{
      int index = 0;
      ObjectNode rootNode = jsonObjectMapper.createObjectNode();
      ArrayNode entriesNode = jsonObjectMapper.createArrayNode();
      for (final QueryContextElement elem : elements) {
        ObjectNode node = serializeQueryContextElement(elem, jsonObjectMapper);
        if(index == 0){
          // The first entry is the main entry
          rootNode.set("main_entry", node);
        }else{
          entriesNode.add(node);
        }
        index++;
      }
      if(index > 1){
        // Add the entries array only if there are more than one entry
        rootNode.set("entries", entriesNode);
      }

    return jsonObjectMapper.writeValueAsString(rootNode);

    }catch (Exception e) {
        logger.debug("serializeQueryContextJson(): Exception {}", e.getMessage());
        return null;
      }


  } // Synchronized
}

private ObjectNode serializeQueryContextElement(QueryContextElement entry, ObjectMapper jsonObjectMapper) throws IOException {
  ObjectNode entryNode = jsonObjectMapper.createObjectNode();

  entryNode.put("id", entry.getId());
  entryNode.put("timestamp", entry.getReadTimestamp());
  entryNode.put("priority", entry.getPriority());
  
  byte[] contextBytes = entry.getContext();
  if (contextBytes != null) {
      String contextStr = new String(contextBytes);
      entryNode.put("context", contextStr);
  }

  return entryNode;
}

  /**
   * @param id the id of the element
   * @param timestamp the last update timestamp
   * @param priority the priority of the element
   * @param opaqueContext the binary data of the opaque context
   * @return a query context element
   */
  private static QueryContextElement createElement(
      long id, long timestamp, long priority, byte[] opaqueContext) {
    return new QueryContextElement(id, timestamp, priority, opaqueContext);
  }

  /**
   * Add an element in the cache.
   *
   * @param qce element to add
   */
  private void addQCE(QueryContextElement qce) {
    idMap.put(qce.id, qce);
    priorityMap.put(qce.priority, qce);
    treeSet.add(qce);
  }

  /**
   * Remove an element from the cache.
   *
   * @param qce element to remove.
   */
  private void removeQCE(QueryContextElement qce) {
    treeSet.remove(qce);
    priorityMap.remove(qce.priority);
    idMap.remove(qce.id);
  }

  /**
   * Replace the cache element with a new response element. Remove old element exist in the cache
   * and add a new element received.
   *
   * @param oldQCE an element exist in the cache
   * @param newQCE a new element just received.
   */
  private void replaceQCE(QueryContextElement oldQCE, QueryContextElement newQCE) {
    // Remove old element from the cache
    removeQCE(oldQCE);
    // Add new element in the cache
    addQCE(newQCE);
  }

  /**
   * Get all elements in the cache in the order of the priority.
   *
   * @return TreeSet containing cache elements
   */
  private TreeSet<QueryContextElement> getElements() {
    return treeSet;
  }

  int getSize() {
    return treeSet.size();
  }

  void getElements(long[] ids, long[] readTimestamps, long[] priorities, byte[][] contexts) {
    TreeSet<QueryContextElement> elems = getElements();
    int i = 0;

    for (QueryContextElement elem : elems) {
      ids[i] = elem.id;
      readTimestamps[i] = elem.readTimestamp;
      priorities[i] = elem.priority;
      contexts[i] = elem.context;
      i++;
    }
  }

  /** Debugging purpose, log the all entries in the cache. */
  void logCacheEntries() {
    if (logger.isDebugEnabled()) {
      TreeSet<QueryContextElement> elements = getElements();
      for (final QueryContextElement elem : elements) {
        logger.debug(
            " Cache Entry: id: {} readTimestamp: {} priority: {}",
            elem.id,
            elem.readTimestamp,
            elem.priority);
      }
    }
  }

  /** Query context information. */
  private static class QueryContextElement implements Comparable<QueryContextElement> {
    long id; // database id as key. (bigint)
    long readTimestamp; // When the query context read (bigint). Compare for same id.
    long priority; // Priority of the query context (bigint). Compare for different ids.
    byte[] context; // Opaque information (varbinary).

    // Constructor with empty input
    public QueryContextElement(){

    }
    /**
     * Constructor.
     *
     * @param id database id
     * @param readTimestamp Server time when this entry read
     * @param priority Priority of this entry w.r.t other ids
     * @param context Opaque query context, used by query processor in the server.
     */
    public QueryContextElement(long id, long readTimestamp, long priority, byte[] context) {
      this.id = id;
      this.readTimestamp = readTimestamp;
      this.priority = priority;
      this.context = context;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof QueryContextElement)) {
        return super.equals(obj);
      }

      QueryContextElement other = (QueryContextElement) obj;
      return (id == other.id
          && readTimestamp == other.readTimestamp
          && priority == other.priority
          && Arrays.equals(context, other.context));
    }

    @Override
    public int hashCode() {
      int hash = 31;

      hash = hash * 31 + (int) id;
      hash += (hash * 31) + (int) readTimestamp;
      hash += (hash * 31) + (int) priority;
      hash += (hash * 31) + Arrays.hashCode(context);

      return hash;
    }

    /**
     * Keep elements in ascending order of the priority. This method called by TreeSet.
     *
     * @param obj the object to be compared.
     * @return 0 if equals, -1 if this element is less than new element, otherwise 1.
     */
    public int compareTo(QueryContextElement obj) {
      return (priority == obj.priority) ? 0 : (((priority - obj.priority) < 0) ? -1 : 1);
    }

    public void setId(long id) {
      this.id = id;
    }

    public void setPriority(long priority) {
      this.priority = priority;
    }

    public void setContext(byte[] context) {
      this.context = context;
    }

    public void setReadTimestamp(long readTimestamp) {
      this.readTimestamp = readTimestamp;
    }

    public long getId() {
      return id;
    }

    public long getReadTimestamp() {
      return readTimestamp;
    }

    public long getPriority() {
      return priority;
    }

    public byte[] getContext() {
      return context;
    }
  }
}
