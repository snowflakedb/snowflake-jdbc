package net.snowflake.client.internal.core.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import net.snowflake.client.TestUtil;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class IpAddressConverterTest extends BaseConverterTest {
  private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  @AfterEach
  public void closeAllocator() {
    allocator.close();
  }

  private VarCharVector ipAddressVector(String... values) {
    Map<String, String> metadata = Collections.singletonMap("logicalType", "IPADDRESS");
    FieldType fieldType = new FieldType(true, Types.MinorType.VARCHAR.getType(), null, metadata);
    VarCharVector vector = new VarCharVector("ip", fieldType, allocator);
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        vector.setNull(i);
      } else {
        vector.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
      }
    }
    vector.setValueCount(values.length);
    return vector;
  }

  @Test
  public void testIpAddressLogicalTypeDispatchesToIpAddressConverter() throws Throwable {
    try (VarCharVector vector = ipAddressVector("10.0.0.1")) {
      assertInstanceOf(
          IpAddressConverter.class, ArrowVectorConverterUtil.initConverter(vector, this, null, 0));
    }
  }

  @Test
  public void testConvertToInetAddress() throws Throwable {
    try (VarCharVector vector = ipAddressVector("192.168.0.1", null, "2001:db8::1")) {
      ArrowVectorConverter converter = new IpAddressConverter(vector, 0, this);

      Object ipv4 = converter.toObject(0);
      assertInstanceOf(Inet4Address.class, ipv4);
      assertEquals(InetAddress.getByName("192.168.0.1"), ipv4);
      assertEquals("192.168.0.1", converter.toString(0));

      assertTrue(converter.isNull(1));
      assertNull(converter.toObject(1));
      assertNull(converter.toString(1));

      Object ipv6 = converter.toObject(2);
      assertInstanceOf(Inet6Address.class, ipv6);
      assertEquals(InetAddress.getByName("2001:db8::1"), ipv6);
      assertEquals("2001:db8::1", converter.toString(2));
    }
  }

  @Test
  public void testInvalidTextAndUnsupportedConversionsThrow() {
    try (VarCharVector vector = ipAddressVector("not-an-ip")) {
      ArrowVectorConverter converter = new IpAddressConverter(vector, 0, this);

      TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toObject(0));
      TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toInt(0));
    }
  }
}
