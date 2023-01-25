/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.TreeSet;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Most Recently Used and Priority based cache. A separate cache for each connection in the driver.
 */
public class QueryContextCache {
  private final int capacity; // Capacity of the cache
  private final HashMap<Long, QueryContextElement> idMap; // Map for id and QCC

  private final TreeSet<QueryContextElement> treeSet; // Order data as per priority

  private final HashMap<Long, QueryContextElement> priorityMap; // Map for priority and QCC

  private static final SFLogger logger = SFLoggerFactory.getLogger(QueryContextCache.class);

  // Arrow format position
  private static final int ID_POS = 0;
  private static final int TIMESTAMP_POS = 1;
  private static final int PRIORITY_POS = 2;
  private static final int CONTEXT_POS = 3;

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
   * Deserialize the query context from the base64 encoded arrow data.
   *
   * @param data the base64 encoded query context that was serialized using Arrow.
   */
  public void deserializeFromArrowBase64(String data) {
    synchronized (this) {
      logger.debug("deserializeFromArrowBase64() called: data from the server = {}", data);

      // Log existing cache entries
      logCacheEntries();

      if (data == null || data.length() == 0) {
        // Clear the cache
        clearCache();

        logger.debug("deserializeFromArrowBase64() returns");
        // Log existing cache entries
        logCacheEntries();

        return;
      }

      byte[] decoded = Base64.getDecoder().decode(data);
      ByteArrayInputStream input = new ByteArrayInputStream(decoded);
      try (ArrowStreamReader reader = new ArrowStreamReader(input, new RootAllocator())) {
        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          for (int i = 0; i < root.getRowCount(); ++i) {
            QueryContextElement qce = deserializeEntry(root, i);
            // Merge the element in the existing cache
            merge(qce.id, qce.readTimestamp, qce.priority, qce.context);
          }
        }
      } catch (Exception e) {
        logger.debug("deserializeFromArrowBase64: Exception = {}", e.getMessage());
        // Not rethrowing. clear the cache as incomplete merge can lead to unexpected behavior.
        clearCache();
      }

      // After merging all entries, truncate to capacity
      checkCacheCapacity();

      logger.debug("deserializeFromArrowBase64() returns");
      // Log existing cache entries
      logCacheEntries();
    } // Synchronized
  }

  /**
   * Deserialize a query context entry from an Arrow vector
   *
   * @param root the Arrow vector root
   * @param pos the position of the entry to be deserialized from
   * @return a deserialized query context entry
   */
  private static QueryContextElement deserializeEntry(VectorSchemaRoot root, int pos) {
    BigIntVector idVector = (BigIntVector) root.getVector(ID_POS);
    BigIntVector tsVector = (BigIntVector) root.getVector(TIMESTAMP_POS);
    BigIntVector priorityVector = (BigIntVector) root.getVector(PRIORITY_POS);
    VarBinaryVector contextVector = (VarBinaryVector) root.getVector(CONTEXT_POS);

    return createElement(
        idVector.get(pos),
        tsVector.get(pos),
        priorityVector.get(pos),
        contextVector.isNull(pos) ? null : contextVector.get(pos));
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
   * Serialize a query context into a base64 encoded string using Arrow. This will be used to send
   * the serialized query context to Snowflake clients.
   *
   * @return If succeeds, the base64 encoded representation of the query context. Otherwise, return
   *     null.
   */
  public String serializeToArrowBase64() {
    synchronized (this) {
      logger.debug("serializeToArrowBase64() called");
      // Log existing cache entries
      logCacheEntries();

      TreeSet<QueryContextElement> elements = getElements();

      if (elements.size() == 0) return null;
      try (VectorSchemaRoot root =
          VectorSchemaRoot.create(QUERY_CONTEXT_SCHEMA, new RootAllocator())) {
        root.setRowCount(elements.size());
        int pos = 0;
        for (final QueryContextElement elem : elements) {
          serializeEntry(root, elem, pos++);
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }

        String data = Base64.getEncoder().encodeToString(out.toByteArray());

        logger.debug("serializeToArrowBase64(): data to send to server {}", data);

        return data;
      } catch (Exception e) {
        logger.debug("serializeToArrowBase64(): Exception {}", e.getMessage());
        return null;
      }
    } // Synchronized
  }

  /**
   * A utility function to create an arrow field
   *
   * @param fieldName the name of the field
   * @param arrowType the type of the field
   * @param nullable whether the field is nullable
   * @return the created arrow field
   */
  private static Field createArrowField(String fieldName, ArrowType arrowType, boolean nullable) {
    FieldType fieldType = new FieldType(nullable, arrowType, null);
    return new Field(fieldName, fieldType, null);
  }

  /**
   * Serialize the query context entry into the Arrow vector.
   *
   * @param root the Arrow vector root. It must have been pre-allocated.
   * @param elem the query context entry to be serialized
   * @param pos the position where the entry should be serialized to.
   */
  private static void serializeEntry(VectorSchemaRoot root, QueryContextElement elem, int pos) {
    BigIntVector idVector = (BigIntVector) root.getVector(ID_POS);
    BigIntVector tsVector = (BigIntVector) root.getVector(TIMESTAMP_POS);
    BigIntVector priorityVector = (BigIntVector) root.getVector(PRIORITY_POS);
    VarBinaryVector contextVector = (VarBinaryVector) root.getVector(CONTEXT_POS);

    idVector.set(pos, elem.id);
    tsVector.set(pos, elem.readTimestamp);
    priorityVector.set(pos, elem.priority);
    if (elem.context != null) {
      contextVector.setSafe(pos, elem.context);
    } else {
      contextVector.setNull(pos);
    }
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
            " Cache Entry: id: {} readTimestamp: {} priority: {} ",
            elem.id,
            elem.readTimestamp,
            elem.priority);
      }
    }
  }

  /**
   * Note that this schema has been shared to the GS. Changing this may break The query context
   * contains a list of <id, timestamp, priority, opaque context> tuples. The client will maintain
   * the lists locally to maintain the latest opaque context for each id, and perform eviction based
   * on priority (lower numbers have high priorities) to make sure the list size is bounded.
   */
  private static final Schema QUERY_CONTEXT_SCHEMA =
      new Schema(
          Arrays.asList(
              createArrowField("id", Types.MinorType.BIGINT.getType(), false),
              createArrowField("timestamp", Types.MinorType.BIGINT.getType(), false),
              createArrowField("priority", Types.MinorType.BIGINT.getType(), false),
              createArrowField("context", Types.MinorType.VARBINARY.getType(), true)));

  /** Query context information. */
  private static class QueryContextElement implements Comparable<QueryContextElement> {
    long id; // database id as key. (bigint)
    long readTimestamp; // When the query context read (bigint). Compare for same id.
    long priority; // Priority of the query context (bigint). Compare for different ids.
    byte[] context; // Opaque information (varbinary).

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
  }
}
