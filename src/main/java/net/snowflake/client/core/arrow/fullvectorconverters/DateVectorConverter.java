package net.snowflake.client.core.arrow.fullvectorconverters;

import java.util.TimeZone;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.ValueVector;

@SnowflakeJdbcInternalApi
public class DateVectorConverter extends SimpleArrowFullVectorConverter<DateDayVector> {
  private TimeZone timeZone;

  public DateVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      int idx,
      TimeZone timeZone) {
    super(allocator, vector, context, session, idx);
    this.timeZone = timeZone;
  }

  @Override
  protected boolean matchingType() {
    return vector instanceof DateDayVector;
  }

  @Override
  protected DateDayVector initVector() {
    DateDayVector resultVector = new DateDayVector(vector.getName(), allocator);
    resultVector.allocateNew(vector.getValueCount());
    return resultVector;
  }

  @Override
  protected void additionalConverterInit(ArrowVectorConverter converter) {
    if (timeZone != null) {
      converter.setSessionTimeZone(timeZone);
      converter.setUseSessionTimezone(true);
    }
  }

  @Override
  protected void convertValue(ArrowVectorConverter from, DateDayVector to, int idx)
      throws SFException {
    to.set(idx, (int) (from.toDate(idx, null, false).getTime() / (1000 * 3600 * 24)));
  }
}
