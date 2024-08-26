package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.arrow.vector.FieldVector;

public interface ArrowFullVectorConverter {
  FieldVector convert() throws SFException, SnowflakeSQLException;
}
