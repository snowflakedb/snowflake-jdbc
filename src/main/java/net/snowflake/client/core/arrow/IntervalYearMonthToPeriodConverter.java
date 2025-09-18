package net.snowflake.client.core.arrow;

import java.time.Period;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.ValueVector;

class IntervalYearMonthToPeriodConverter extends AbstractArrowVectorConverter {

  private SmallIntVector smallIntVector;
  private IntVector intVector;
  private BigIntVector bigIntVector;

  public IntervalYearMonthToPeriodConverter(
      ValueVector vector, int idx, DataConversionContext context) {
    super(SnowflakeType.INTERVAL_YEAR_MONTH.name(), vector, idx, context);
    if (vector instanceof SmallIntVector) {
      // Underlying Interval Year-Month type is SB2
      this.smallIntVector = (SmallIntVector) vector;
    } else if (vector instanceof IntVector) {
      // Underlying Interval Year-Month type is SB4
      this.intVector = (IntVector) vector;
    } else if (vector instanceof BigIntVector) {
      // Underlying Interval Year-Month type is SB8
      this.bigIntVector = (BigIntVector) vector;
    }
  }

  @Override
  public Period toPeriod(int index) {
    if (isNull(index)) {
      return null;
    }
    if (smallIntVector != null) {
      return Period.ofMonths(smallIntVector.get(index));
    } else if (intVector != null) {
      return Period.ofMonths(intVector.get(index));
    } else {
      return Period.ofMonths((int) bigIntVector.get(index));
    }
  }

  @Override
  public String toString(int index) throws SFException {
    if (isNull(index)) {
      return null;
    }
    return toPeriod(index).toString();
  }

  @Override
  public Object toObject(int index) throws SFException {
    return toPeriod(index);
  }
}
