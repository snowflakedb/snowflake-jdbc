package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.core.ObjectMapperFactory;

/** A class to handle S3 material descriptor metadata entries (matdesc). */
public class MatDesc {
  /** MatDesc key for query ID */
  public static String QUERY_ID = "queryId";

  /** MatDesc key for stage master key ID */
  public static String SMK_ID = "smkId";

  /** MatDesc key for the length of the key in bits */
  public static String KEY_SIZE = "keySize";

  /** If key size is not explicitly specified, assume DEFAULT_KEY_SIZE */
  public static int DEFAULT_KEY_SIZE = 256;

  /** The JSON parser for matdesc entries */
  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

  /** The Stage Master Key ID */
  private final long smkId;

  /** The query ID */
  private final String queryId;

  /** The key length in bits */
  private final int keySize;

  public MatDesc(long smkId, String queryId, int keySize) {
    this.smkId = smkId;
    this.queryId = queryId;
    this.keySize = keySize;
  }

  public MatDesc(long smkId, String queryId) {
    this(smkId, queryId, DEFAULT_KEY_SIZE);
  }

  public long getSmkId() {
    return this.smkId;
  }

  public String getQueryId() {
    return this.queryId;
  }

  public int getKeySize() {
    return this.keySize;
  }

  /**
   * Try to parse the material descriptor string.
   *
   * @param matdesc string
   * @return The material description or null
   */
  public static MatDesc parse(String matdesc) {
    if (matdesc == null) {
      return null;
    }
    try {
      JsonNode jsonNode = mapper.readTree(matdesc);
      JsonNode queryIdNode = jsonNode.path(QUERY_ID);
      if (queryIdNode.isMissingNode() || queryIdNode.isNull()) {
        return null;
      }
      JsonNode smkIdNode = jsonNode.path(SMK_ID);
      if (smkIdNode.isMissingNode() || smkIdNode.isNull()) {
        return null;
      }
      String queryId = queryIdNode.asText();
      long smkId = smkIdNode.asLong();
      JsonNode keySizeNode = jsonNode.path(KEY_SIZE);
      if (!keySizeNode.isMissingNode() && !keySizeNode.isNull()) {
        return new MatDesc(smkId, queryId, keySizeNode.asInt());
      }
      return new MatDesc(smkId, queryId);
    } catch (Exception ex) {
      return null;
    }
  }

  @Override
  public String toString() {
    ObjectNode obj = mapper.createObjectNode();
    obj.put(QUERY_ID, this.queryId);
    obj.put(SMK_ID, Long.toString(this.smkId));
    obj.put(KEY_SIZE, Integer.toString(this.keySize));
    return obj.toString();
  }
}
