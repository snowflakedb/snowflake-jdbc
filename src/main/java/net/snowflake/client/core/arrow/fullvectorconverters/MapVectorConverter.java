package net.snowflake.client.core.arrow.fullvectorconverters;


import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;

public class MapVectorConverter extends ListVectorConverter {

    MapVectorConverter(RootAllocator allocator, ValueVector vector, DataConversionContext context, SFBaseSession session, int idx, Object valueTargetType) {
        super(allocator, vector, context, session, idx, valueTargetType);
    }

    @Override
    protected ListVector initVector(String name, Field field) {
        MapVector convertedMapVector = MapVector.empty(name, allocator, false);
        ArrayList<Field> fields = new ArrayList<>();
        fields.add(field);
        convertedMapVector.initializeChildrenFromFields(fields);
        return convertedMapVector;
    }
}
