package net.snowflake.client.internal.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class IpAddressParserTest {

  @Test
  public void testParsesIpv4AndIpv6Literals() throws Exception {
    InetAddress ipv4 = IpAddressParser.parse("10.0.255.1");
    assertInstanceOf(Inet4Address.class, ipv4);
    assertEquals(InetAddress.getByName("10.0.255.1"), ipv4);

    InetAddress ipv6 = IpAddressParser.parse("fe80::1");
    assertInstanceOf(Inet6Address.class, ipv6);
    assertEquals(InetAddress.getByName("fe80::1"), ipv6);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "not-an-ip",
        "localhost",
        "256.0.0.1",
        "10.0.0",
        "1.2.3.4.5",
        "1..2.3",
        "2001:db8::zz",
        ""
      })
  public void testRejectsInvalidLiteralsWithoutDnsLookup(String text) {
    assertNull(IpAddressParser.parse(text));
  }

  @ParameterizedTest
  @ValueSource(strings = {"10.0.255.1", "fe80::1"})
  public void testGetHostNameReturnsLiteralWithoutDnsLookup(String text) {
    // getHostName() only performs a reverse DNS lookup when the hostname is unset; asserting it
    // returns the literal confirms parse() stored it via getByAddress(text, bytes) instead.
    assertEquals(text, IpAddressParser.parse(text).getHostName());
  }
}
