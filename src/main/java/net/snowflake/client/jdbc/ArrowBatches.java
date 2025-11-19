package net.snowflake.client.jdbc;

import java.sql.SQLException;

public interface ArrowBatches {
  boolean hasNext();

  ArrowBatch next() throws SQLException;

  long getRowCount() throws SQLException;
}
