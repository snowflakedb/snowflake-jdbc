package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.arrow.vector.FieldVector;

public abstract class AbstractFullVectorConverter implements ArrowFullVectorConverter {
  private boolean converted;

  protected abstract FieldVector convertVector()
      throws SFException, SnowflakeSQLException, SFArrowException;

  @Override
  public FieldVector convert() throws SFException, SnowflakeSQLException, SFArrowException {
    if (converted) {
      throw new SFArrowException(
          ArrowErrorCode.VECTOR_ALREADY_CONVERTED, "Convert has already been called");
    } else {
      converted = true;
      return convertVector();
    }
  }
}
