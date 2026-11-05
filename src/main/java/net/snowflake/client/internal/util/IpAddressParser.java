package net.snowflake.client.internal.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

/** Parses IPv4/IPv6 literals into {@link InetAddress} values without any DNS lookup. */
public final class IpAddressParser {
  /** Java type name reported in IPADDRESS conversion errors. */
  public static final String INET_ADDRESS_STR = "InetAddress";

  private IpAddressParser() {}

  /**
   * @param text an IPv4 or IPv6 address literal
   * @return the parsed {@link java.net.Inet4Address} or {@link java.net.Inet6Address}, or null if
   *     {@code text} is not a valid address literal
   */
  public static InetAddress parse(String text) {
    try {
      if (text.indexOf(':') >= 0) {
        // A bracketed host is parsed strictly as an IPv6 literal and never resolved through DNS.
        byte[] ipv6 = InetAddress.getByName("[" + text + "]").getAddress();
        // getByAddress(text, ipv6) stores the literal as the hostname so a later getHostName()
        // does not reverse-resolve it via DNS.
        return InetAddress.getByAddress(text, ipv6);
      }
      byte[] ipv4 = parseIpv4(text);
      return ipv4 == null ? null : InetAddress.getByAddress(text, ipv4);
    } catch (UnknownHostException e) {
      return null;
    }
  }

  private static byte[] parseIpv4(String text) {
    String[] octets = text.split("\\.", -1);
    if (octets.length != 4) {
      return null;
    }
    byte[] bytes = new byte[4];
    for (int i = 0; i < 4; i++) {
      String octet = octets[i];
      if (octet.isEmpty() || octet.length() > 3) {
        return null;
      }
      int value = 0;
      for (int j = 0; j < octet.length(); j++) {
        char c = octet.charAt(j);
        if (c < '0' || c > '9') {
          return null;
        }
        value = value * 10 + (c - '0');
      }
      if (value > 255) {
        return null;
      }
      bytes[i] = (byte) value;
    }
    return bytes;
  }
}
