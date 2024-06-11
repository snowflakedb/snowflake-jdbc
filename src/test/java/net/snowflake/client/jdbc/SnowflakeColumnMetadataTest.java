package net.snowflake.client.jdbc;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;


public class SnowflakeColumnMetadataTest {

    @Test
    public void testSnowflakeColumnMetaData() {
        SnowflakeColumnMetadata metadata = new SnowflakeColumnMetadata("mock",1,true,2,3,4,"fake",true,null,null,null,null,true);
        List<FieldMetadata> list = mock(List.class);
        metadata.setName("test");
        metadata.setType(10);
        metadata.setNullable(false);
        metadata.setLength(12);
        metadata.setPrecision(13);
        metadata.setScale(14);
        metadata.setTypeName("type");
        metadata.setFixed(false);
        metadata.setFields(list);
        metadata.setAutoIncrement(false);

        assertEquals(metadata.getName(),"test");
        assertEquals(metadata.getType(),10);
        assertFalse(metadata.isNullable());
        assertEquals(metadata.getLength(),12);
        assertEquals(metadata.getPrecision(),13);
        assertEquals(metadata.getScale(),14);
        assertEquals(metadata.getTypeName(),"type");
        assertFalse(metadata.isFixed());
        assertEquals(metadata.getFields(),list);
        assertFalse(metadata.isAutoIncrement());
    }
}
