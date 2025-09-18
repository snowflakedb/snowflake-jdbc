package net.snowflake.client.core.arrow;

import java.time.Period;
import java.time.Duration;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;

class IntervalYearMonthToPeriodConverter extends AbstractArrowVectorConverter {

    private BigIntVector vector;

    public IntervalYearMonthToPeriodConverter(ValueVector vector, int idx, DataConversionContext context) {
        super(SnowflakeType.INTERVAL_YEAR_MONTH.name(), vector, idx, context);
        this.vector = (BigIntVector) vector;
    }

    @Override
    public Period toPeriod(int index) {
        if (isNull(index)) {
            return null;
        }
        return Period.ofMonths((int) vector.get(index));
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
