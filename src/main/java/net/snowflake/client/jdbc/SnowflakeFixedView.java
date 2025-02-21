package net.snowflake.client.jdbc;

import java.util.List;
import net.snowflake.client.core.SFBaseSession;

/** An interface to use for returning query results from any java class */
public interface SnowflakeFixedView {
  List<SnowflakeColumnMetadata> describeColumns(SFBaseSession session) throws Exception;

  List<Object> getNextRow() throws Exception;

  int getTotalRows();
}
