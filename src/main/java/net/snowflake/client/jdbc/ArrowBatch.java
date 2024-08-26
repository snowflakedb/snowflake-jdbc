package net.snowflake.client.jdbc;

import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;

public interface ArrowBatch {
  List<VectorSchemaRoot> fetch() throws SnowflakeSQLException;

  long getRowCount();
}
