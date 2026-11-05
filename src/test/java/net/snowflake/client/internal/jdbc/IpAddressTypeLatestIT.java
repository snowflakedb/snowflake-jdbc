package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * IPADDRESS values come back as {@link InetAddress} when the server returns native IPADDRESS result
 * metadata. Servers that return IPADDRESS columns as TEXT skip this class.
 */
@Tag(TestTags.RESULT_SET)
public class IpAddressTypeLatestIT extends BaseJDBCWithSharedConnectionIT {
  private static final int LARGE_RESULT_SET_SIZE = 20_000;

  @BeforeAll
  public static void requireNativeIpAddress() throws SQLException {
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT '10.0.0.1'::IPADDRESS")) {
      assumeTrue(
          "IPADDRESS".equals(rs.getMetaData().getColumnTypeName(1)),
          "server returns IPADDRESS columns as TEXT");
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testReturnsInetAddressObjects(String queryResultFormat) throws Exception {
    try (Statement stmt = createStatement(queryResultFormat);
        ResultSet rs =
            stmt.executeQuery(
                "SELECT '192.168.0.1'::IPADDRESS, '2001:db8::1'::IPADDRESS, NULL::IPADDRESS")) {
      assertTrue(rs.next());

      Object ipv4 = rs.getObject(1);
      assertInstanceOf(Inet4Address.class, ipv4);
      assertEquals(InetAddress.getByName("192.168.0.1"), ipv4);
      assertEquals("192.168.0.1", rs.getString(1));

      Object ipv6 = rs.getObject(2);
      assertInstanceOf(Inet6Address.class, ipv6);
      assertEquals(InetAddress.getByName("2001:db8::1"), ipv6);
      assertEquals("2001:db8::1", rs.getString(2));

      assertNull(rs.getObject(3));
      assertTrue(rs.wasNull());
      assertNull(rs.getString(3));
      assertFalse(rs.next());
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGetObjectWithInetAddressClass(String queryResultFormat) throws Exception {
    try (Statement stmt = createStatement(queryResultFormat);
        ResultSet rs =
            stmt.executeQuery(
                "SELECT '192.168.0.1'::IPADDRESS, '2001:db8::1'::IPADDRESS, NULL::IPADDRESS,"
                    + " '10.0.0.1'::VARCHAR")) {
      assertTrue(rs.next());
      assertEquals(InetAddress.getByName("192.168.0.1"), rs.getObject(1, InetAddress.class));
      assertEquals(InetAddress.getByName("192.168.0.1"), rs.getObject(1, Inet4Address.class));
      assertEquals(InetAddress.getByName("2001:db8::1"), rs.getObject(2, Inet6Address.class));
      assertThrows(SQLException.class, () -> rs.getObject(2, Inet4Address.class));
      assertNull(rs.getObject(3, InetAddress.class));
      assertTrue(rs.wasNull());
      assertThrows(SQLException.class, () -> rs.getObject(4, InetAddress.class));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testReturnsCanonicalForm(String queryResultFormat) throws Exception {
    try (Statement stmt = createStatement(queryResultFormat);
        ResultSet rs =
            stmt.executeQuery(
                "SELECT '2001:0DB8:0000:0000:0000:0000:0000:0001'::IPADDRESS,"
                    + " '::ffff:10.0.0.1'::IPADDRESS")) {
      assertTrue(rs.next());
      assertEquals("2001:db8::1", rs.getString(1));
      assertEquals(InetAddress.getByName("2001:db8::1"), rs.getObject(1));
      assertEquals("10.0.0.1", rs.getString(2));
      assertEquals(InetAddress.getByName("10.0.0.1"), rs.getObject(2));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDescribesIpAddressColumns(String queryResultFormat) throws Exception {
    try (Statement stmt = createStatement(queryResultFormat);
        ResultSet rs = stmt.executeQuery("SELECT '10.0.0.1'::IPADDRESS AS ip")) {
      ResultSetMetaData meta = rs.getMetaData();
      assertEquals("IP", meta.getColumnName(1));
      assertEquals(Types.OTHER, meta.getColumnType(1));
      assertEquals("IPADDRESS", meta.getColumnTypeName(1));
      assertEquals(InetAddress.class.getName(), meta.getColumnClassName(1));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testReturnsInetAddressObjectsFromLargeResult(String queryResultFormat)
      throws Exception {
    try (Statement stmt = createStatement(queryResultFormat);
        ResultSet rs =
            stmt.executeQuery(
                "SELECT ('10.0.' || (seq4() % 256) || '.' || (seq4() % 250))::IPADDRESS"
                    + " FROM TABLE(GENERATOR(ROWCOUNT => "
                    + LARGE_RESULT_SET_SIZE
                    + "))")) {
      int rows = 0;
      while (rs.next()) {
        assertInstanceOf(Inet4Address.class, rs.getObject(1));
        rows++;
      }
      assertEquals(LARGE_RESULT_SET_SIZE, rows);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testRoundTripsBoundInetAddress(String queryResultFormat) throws Exception {
    try (Statement stmt = createStatement(queryResultFormat)) {
      for (String literal : new String[] {"172.16.0.1", "fe80::1"}) {
        InetAddress value = InetAddress.getByName(literal);
        try (PreparedStatement ps = connection.prepareStatement("SELECT ?::IPADDRESS")) {
          ps.setObject(1, value);
          try (ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(value, rs.getObject(1));
          }
        }
      }
    }
  }
}
