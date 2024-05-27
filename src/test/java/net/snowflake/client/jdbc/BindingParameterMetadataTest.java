package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class BindingParameterMetadataTest {

  @Test
  public void testBindParameterWithVariousConstructors() {
    BindingParameterMetadata bindingParameterMetadata = new BindingParameterMetadata();
    bindingParameterMetadata.setType("FLOAT");
    bindingParameterMetadata.setLength(100);
    bindingParameterMetadata.setByteLength(1000);
    bindingParameterMetadata.setPrecision(255);
    bindingParameterMetadata.setScale(20);
    bindingParameterMetadata.setNullable(false);
    assertEquals(bindingParameterMetadata.getType(), "FLOAT");
    assertEquals(bindingParameterMetadata.getLength(), Integer.valueOf(100));
    assertEquals(bindingParameterMetadata.getByteLength(), Integer.valueOf(1000));
    assertEquals(bindingParameterMetadata.getPrecision(), Integer.valueOf(255));
    assertEquals(bindingParameterMetadata.getScale(), Integer.valueOf(20));
    assertEquals(bindingParameterMetadata.isNullable(), false);

    BindingParameterMetadata bindingParameterMetadata1 =
        new BindingParameterMetadata("STRING", "NAME");
    assertEquals(bindingParameterMetadata1.getName(), "NAME");
    assertEquals(bindingParameterMetadata1.getType(), "STRING");

    BindingParameterMetadata bindingParameterMetadata2 =
        new BindingParameterMetadata("STRING", "NAME", 100, 1000, 255, 20, true);
    assertEquals(bindingParameterMetadata2.getType(), "STRING");
    assertEquals(bindingParameterMetadata2.getName(), "NAME");
    assertEquals(bindingParameterMetadata2.getLength(), Integer.valueOf(100));
    assertEquals(bindingParameterMetadata2.getByteLength(), Integer.valueOf(1000));
    assertEquals(bindingParameterMetadata2.getPrecision(), Integer.valueOf(255));
    assertEquals(bindingParameterMetadata2.getScale(), Integer.valueOf(20));
    assertEquals(bindingParameterMetadata2.isNullable(), true);

    BindingParameterMetadata bindingParameterMetadata3 =
        BindingParameterMetadata.BindingParameterMetadataBuilder.bindingParameterMetadata()
            .withNullable(true)
            .build();
    assertEquals(bindingParameterMetadata3.isNullable(), true);
  }
}
