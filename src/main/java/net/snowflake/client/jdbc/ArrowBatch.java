package net.snowflake.client.jdbc;

import java.util.List;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public interface ArrowBatch {
  List<VectorSchemaRoot> fetch() throws SnowflakeSQLException;

  ArrowVectorConverter getTimestampConverter(FieldVector vector, int colIdx);

  long getRowCount();
}
