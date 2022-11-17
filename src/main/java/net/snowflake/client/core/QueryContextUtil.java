/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
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

/** A utility class to serialize/deserialize QueryContext. */
public class QueryContextUtil {

  private static final int ID_POS = 0;
  private static final int TIMESTAMP_POS = 1;
  private static final int PRIORITY_POS = 2;
  private static final int CONTEXT_POS = 3;

  static final SFLogger logger = SFLoggerFactory.getLogger(QueryContextUtil.class);

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

  /**
   * Deserialize the query context from the base64 encoded arrow data.
   *
   * @param data the base64 encoded query context that was serialized using Arrow.
   */
  public static void deserializeFromArrowBase64(QueryContextCache qcc, String data) {
    synchronized (qcc) {
      logger.debug("deserializeFromArrowBase64() called: data from the server = {}", data);

      // Log existing cache entries
      qcc.logCacheEntries();

      if (data == null || data.length() == 0) {
        // Clear the cache
        qcc.clearCache();

        logger.debug("deserializeFromArrowBase64() returns");
        // Log existing cache entries
        qcc.logCacheEntries();

        return;
      }

      byte[] decoded = Base64.getDecoder().decode(data);
      ByteArrayInputStream input = new ByteArrayInputStream(decoded);
      try (ArrowStreamReader reader = new ArrowStreamReader(input, new RootAllocator())) {
        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          for (int i = 0; i < root.getRowCount(); ++i) {
            QueryContextElement elem = deserializeEntry(root, i);
            // Merge the element in the existing cache
            qcc.merge(elem.id, elem.readTimestamp, elem.priority, elem.context);
          }
        }
      } catch (Exception e) {
        logger.debug("deserializeFromArrowBase64: Exception = {}", e.getMessage());
        // Not rethrowing
      }

      // After merging all entries, truncate to capacity
      qcc.checkCacheCapacity();

      logger.debug("deserializeFromArrowBase64() returns");
      // Log existing cache entries
      qcc.logCacheEntries();
    } // Synchronized
  }

  /**
   * Deserialize a query context entry from an Arrow vector
   *
   * @param root the Arrow vector root
   * @param pos the position of the entry to be deserialized from
   * @return a deserialized query context entry
   */
  private static QueryContextElement deserializeEntry(VectorSchemaRoot root, int pos)
      throws IOException {
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
      long id, long timestamp, long priority, byte[] opaqueContext) throws IOException {
    return new QueryContextElement(id, timestamp, priority, opaqueContext);
  }

  /**
   * Serialize a query context into a base64 encoded string using Arrow. This will be used to send
   * the serialized query context to Snowflake clients.
   *
   * @param qcc the query context to be serialized.
   * @return If succeeds, the base64 encoded representation of the query context. Otherwise, return
   *     null.
   */
  public static String serializeToArrowBase64(QueryContextCache qcc) {
    synchronized (qcc) {
      logger.debug("serializeToArrowBase64() called");
      // Log existing cache entries
      qcc.logCacheEntries();

      TreeSet<QueryContextElement> elements = qcc.getElements();

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
    contextVector.set(pos, elem.context);
  }
}
