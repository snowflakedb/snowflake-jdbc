package net.snowflake.client.core.arrow;

import java.time.Period;
import java.time.Duration;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;

class IntervalDayTimeToDurationConverter extends AbstractArrowVectorConverter {

    private StructVector vector;

    public IntervalDayTimeToDurationConverter(ValueVector vector, int idx, DataConversionContext context) {
        super(SnowflakeType.INTERVAL_DAY_TIME.name(), vector, idx, context);
        this.vector = (StructVector) vector;
    }

    @Override
    public Duration toDuration(int index) {
        if (isNull(index)) {
            return null;
        }
        return Duration.ofNanos((long) index);
    }

    @Override
    public String toString(int index) throws SFException {
        if (isNull(index)) {
            return null;
        }
        return toDuration(index).toString();
    }

    @Override
    public Object toObject(int index) throws SFException {
        return toDuration(index);
    }
}
