package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
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

  private final HashMap<Long, QueryContextElement>
      newPriorityMap; // Intermediate map for priority and QCC for current round of merging

  private static final SFLogger logger = SFLoggerFactory.getLogger(QueryContextCache.class);

  private static ObjectMapper jsonObjectMapper;

  static {
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
    newPriorityMap = new HashMap<>();
    treeSet =
        new TreeSet<>(
            Comparator.comparingLong(QueryContextElement::getPriority)
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
  void merge(long id, long readTimestamp, long priority, String context) {
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

  /** Sync the newPriorityMap with the priorityMap at the end of current round of merge */
  void syncPriorityMap() {
    logger.debug(
        "syncPriorityMap called priorityMap size: {}, newPrioirtyMap size: {}",
        priorityMap.size(),
        newPriorityMap.size());
    for (Map.Entry<Long, QueryContextElement> entry : newPriorityMap.entrySet()) {
      priorityMap.put(entry.getKey(), entry.getValue());
    }
    // clear the newPriorityMap for next round of QCC merge(a round consists of multiple entries)
    newPriorityMap.clear();
  }

  /**
   * After the merge, loop through priority list and make sure cache is at most capacity. Remove all
   * other elements from the list based on priority.
   */
  void checkCacheCapacity() {
    logger.debug(
        "checkCacheCapacity() called. treeSet size: {} cache capacity: {}",
        treeSet.size(),
        capacity);
    if (treeSet.size() > capacity) {
      // remove elements based on priority
      while (treeSet.size() > capacity) {
        QueryContextElement qce = treeSet.last();
        removeQCE(qce);
      }
    }

    logger.debug(
        "checkCacheCapacity() returns. treeSet size: {} cache capacity: {}",
        treeSet.size(),
        capacity);
  }

  /** Clear the cache. */
  public void clearCache() {
    logger.trace("clearCache() called");
    idMap.clear();
    priorityMap.clear();
    treeSet.clear();
    logger.trace("clearCache() returns. Number of entries in cache now: {}", treeSet.size());
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
        return;
      }

      try {
        JsonNode rootNode = jsonObjectMapper.readTree(data);

        // Deserialize the entries. The first entry with priority is the main entry. On JDBC side,
        // we save all entries into one list to simplify the logic. An example JSON is:
        // {
        //   "entries": [
        //    {
        //     "id": 0,
        //     "read_timestamp": 123456789,
        //     "priority": 0,
        //     "context": "base64 encoded context"
        //    },
        //     {
        //       "id": 1,
        //       "read_timestamp": 123456789,
        //       "priority": 1,
        //       "context": "base64 encoded context"
        //     },
        //     {
        //       "id": 2,
        //       "read_timestamp": 123456789,
        //       "priority": 2,
        //       "context": "base64 encoded context"
        //     }
        //   ]

        JsonNode entriesNode = rootNode.path("entries");
        if (entriesNode != null && entriesNode.isArray()) {
          for (JsonNode entryNode : entriesNode) {
            QueryContextElement entry = deserializeQueryContextElement(entryNode);
            if (entry != null) {
              merge(entry.id, entry.readTimestamp, entry.priority, entry.context);
            } else {
              logger.warn(
                  "deserializeQueryContextJson: deserializeQueryContextElement meets mismatch field type. Clear the QueryContextCache.");
              clearCache();
              return;
            }
          }
          // after merging all entries, sync the internal priority map to priority map. Because of
          // priority swicth from GS side,
          // there could be priority key conflict if we directly operating on the priorityMap during
          // a round of merge.
          syncPriorityMap();
        }
      } catch (Exception e) {
        logger.debug("deserializeQueryContextJson: Exception: {}", e.getMessage());
        // Not rethrowing. clear the cache as incomplete merge can lead to unexpected behavior.
        clearCache();
      }

      // After merging all entries, truncate to capacity
      checkCacheCapacity();

      // Log existing cache entries
      logCacheEntries();
    } // Synchronized
  }

  private static QueryContextElement deserializeQueryContextElement(JsonNode node)
      throws IOException {
    QueryContextElement entry = new QueryContextElement();
    JsonNode idNode = node.path("id");
    if (idNode.isNumber()) {
      entry.setId(idNode.asLong());
    } else {
      logger.warn("deserializeQueryContextElement: `id` field is not Number type");
      return null;
    }

    JsonNode timestampNode = node.path("timestamp");
    if (timestampNode.isNumber()) {
      entry.setReadTimestamp(timestampNode.asLong());
    } else {
      logger.warn("deserializeQueryContextElement: `timestamp` field is not Long type");
      return null;
    }

    JsonNode priorityNode = node.path("priority");
    if (priorityNode.isNumber()) {
      entry.setPriority(priorityNode.asLong());
    } else {
      logger.warn("deserializeQueryContextElement: `priority` field is not Long type");
      return null;
    }

    JsonNode contextNode = node.path("context");
    if (contextNode.isTextual()) {
      String contextBytes = contextNode.asText();
      entry.setContext(contextBytes);
    } else if (contextNode.isEmpty()) {
      // Currenly the OpaqueContext field is empty in the JSON received from GS. In the future, it
      // will
      // be filled with OpaqueContext object in base64 format.
      logger.debug("deserializeQueryContextElement `context` field is empty");
    } else {
      logger.warn("deserializeQueryContextElement: `context` field is not String type");
      return null;
    }

    return entry;
  }

  /**
   * Deserialize the QueryContext cache from a QueryContextDTO object. This function currently is
   * only used in QueryContextCacheTest.java where we check that after serialization and
   * deserialization, the cache is the same as before.
   *
   * @param queryContextDTO QueryContextDTO to deserialize.
   */
  public void deserializeQueryContextDTO(QueryContextDTO queryContextDTO) {
    synchronized (this) {
      // Log existing cache entries
      logCacheEntries();

      if (queryContextDTO == null) {
        // Clear the cache
        clearCache();

        // Log existing cache entries
        logCacheEntries();

        return;
      }

      try {

        List<QueryContextEntryDTO> entries = queryContextDTO.getEntries();
        if (entries != null) {
          for (QueryContextEntryDTO entryDTO : entries) {
            // The main entry priority will always be 0, we simply save a list of
            // QueryContextEntryDTO in QueryContextDTO
            QueryContextElement entry = deserializeQueryContextElementDTO(entryDTO);
            merge(entry.id, entry.readTimestamp, entry.priority, entry.context);
            logCacheEntries();
          }
        }
        // after merging all entries, sync the internal priority map to priority map. Because of
        // priority swicth from GS side,
        // there could be priority key conflict if we directly operating on the priorityMap during a
        // round of merge.
        syncPriorityMap();
      } catch (Exception e) {
        logger.debug("deserializeQueryContextDTO: Exception: {}", e.getMessage());
        // Not rethrowing. clear the cache as incomplete merge can lead to unexpected behavior.
        clearCache();
      }

      // After merging all entries, truncate to capacity
      checkCacheCapacity();

      // Log existing cache entries
      logCacheEntries();
    } // Synchronized
  }

  private static QueryContextElement deserializeQueryContextElementDTO(
      QueryContextEntryDTO entryDTO) throws IOException {
    QueryContextElement entry =
        new QueryContextElement(
            entryDTO.getId(),
            entryDTO.getTimestamp(),
            entryDTO.getPriority(),
            entryDTO.getContext().getBase64Data());
    return entry;
  }

  /**
   * Serialize the QueryContext cache to a QueryContextDTO object, which can be serialized to JSON
   * automatically later.
   *
   * @return {@link QueryContextDTO}
   */
  public QueryContextDTO serializeQueryContextDTO() {
    synchronized (this) {
      // Log existing cache entries
      logCacheEntries();

      TreeSet<QueryContextElement> elements = getElements();
      if (elements.size() == 0) {
        return null;
      }

      try {
        QueryContextDTO queryContextDTO = new QueryContextDTO();
        List<QueryContextEntryDTO> entries = new ArrayList<QueryContextEntryDTO>();
        // the first element is the main entry with priority 0. We use a list of
        // QueryContextEntryDTO to store all entries in QueryContextDTO
        // to simplify the JDBC side QueryContextCache design.
        for (final QueryContextElement elem : elements) {
          QueryContextEntryDTO queryContextElementDTO = serializeQueryContextEntryDTO(elem);
          entries.add(queryContextElementDTO);
        }
        queryContextDTO.setEntries(entries);

        return queryContextDTO;

      } catch (Exception e) {
        logger.debug("serializeQueryContextDTO(): Exception: {}", e.getMessage());
        return null;
      }
    }
  }

  private QueryContextEntryDTO serializeQueryContextEntryDTO(QueryContextElement entry)
      throws IOException {
    // OpaqueContextDTO contains a base64 encoded byte array. On JDBC side, we do not decode and
    // encode it
    QueryContextEntryDTO entryDTO =
        new QueryContextEntryDTO(
            entry.getId(),
            entry.getReadTimestamp(),
            entry.getPriority(),
            new OpaqueContextDTO(entry.getContext()));
    return entryDTO;
  }

  /**
   * @param id the id of the element
   * @param timestamp the last update timestamp
   * @param priority the priority of the element
   * @param opaqueContext the binary data of the opaque context
   * @return a query context element
   */
  private static QueryContextElement createElement(
      long id, long timestamp, long priority, String opaqueContext) {
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

  void getElements(long[] ids, long[] readTimestamps, long[] priorities, String[] contexts) {
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
    String context; // Opaque information (varbinary).

    public QueryContextElement() {
      // Default constructor
    }

    /**
     * Constructor.
     *
     * @param id database id
     * @param readTimestamp Server time when this entry read
     * @param priority Priority of this entry w.r.t other ids
     * @param context Opaque query context, used by query processor in the server.
     */
    public QueryContextElement(long id, long readTimestamp, long priority, String context) {
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
          && context.equals(other.context));
    }

    @Override
    public int hashCode() {
      int hash = 31;

      hash = hash * 31 + (int) id;
      hash += (hash * 31) + (int) readTimestamp;
      hash += (hash * 31) + (int) priority;
      hash += (hash * 31) + context.hashCode();

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

    public void setContext(String context) {
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

    public String getContext() {
      return context;
    }
  }
}
