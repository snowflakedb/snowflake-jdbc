package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(TestTags.STATEMENT)
public class CallableStatementIT extends CallableStatementITBase {

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testPrepareCall(String queryResultFormat) throws SQLException {
    // test CallableStatement with no binding parameters
    try (Connection connection = getConnection(queryResultFormat)) {
      try (CallableStatement callableStatement = connection.prepareCall("call square_it(5)")) {
        assertThat(callableStatement.getParameterMetaData().getParameterCount(), is(0));
      }
      // test CallableStatement with 1 binding parameter
      try (CallableStatement callableStatement = connection.prepareCall("call square_it(?)")) {
        // test that getParameterMetaData works with CallableStatement. At this point, it always
        // returns
        // the type as "text."
        assertThat(callableStatement.getParameterMetaData().getParameterType(1), is(Types.VARCHAR));
        callableStatement.getParameterMetaData().getParameterTypeName(1);
        assertThat(callableStatement.getParameterMetaData().getParameterTypeName(1), is("text"));
        callableStatement.setFloat(1, 7.0f);
        try (ResultSet rs = callableStatement.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(49.0f, rs.getFloat(1), 1.0f);
        }
      }
      // test CallableStatement with 2 binding parameters
      try (CallableStatement callableStatement = connection.prepareCall("call add_nums(?,?)")) {
        callableStatement.setDouble(1, 32);
        callableStatement.setDouble(2, 15);
        try (ResultSet rs = callableStatement.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(47, rs.getDouble(1), .5);
        }
      }
    }
  }

  @Test
  public void testFeatureNotSupportedException() throws Throwable {
    try (Connection connection = getConnection()) {
      CallableStatement callableStatement = connection.prepareCall("select ?");
      expectFeatureNotSupportedException(
          () -> callableStatement.registerOutParameter(1, Types.INTEGER));
      expectFeatureNotSupportedException(
          () -> callableStatement.registerOutParameter(1, Types.INTEGER, 1));
      expectFeatureNotSupportedException(
          () -> callableStatement.registerOutParameter(1, Types.INTEGER, "int"));
      expectFeatureNotSupportedException(
          () -> callableStatement.registerOutParameter("param_name", Types.INTEGER));
      expectFeatureNotSupportedException(
          () -> callableStatement.registerOutParameter("param_name", Types.INTEGER, 1));
      expectFeatureNotSupportedException(
          () -> callableStatement.registerOutParameter("param_name", Types.INTEGER, "int"));
      expectFeatureNotSupportedException(() -> callableStatement.getArray("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getArray(1));
      expectFeatureNotSupportedException(() -> callableStatement.getBigDecimal("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getBigDecimal(1));
      expectFeatureNotSupportedException(() -> callableStatement.getBlob("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getBlob(1));
      expectFeatureNotSupportedException(() -> callableStatement.getBoolean("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getBoolean(1));
      expectFeatureNotSupportedException(() -> callableStatement.getByte("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getByte(1));
      expectFeatureNotSupportedException(() -> callableStatement.getBytes("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getBytes(1));
      expectFeatureNotSupportedException(() -> callableStatement.getCharacterStream("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getCharacterStream(1));
      expectFeatureNotSupportedException(() -> callableStatement.getClob("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getClob(1));
      expectFeatureNotSupportedException(() -> callableStatement.getDate("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getDate(1));
      expectFeatureNotSupportedException(
          () -> callableStatement.getDate("param_name", Calendar.getInstance()));
      expectFeatureNotSupportedException(
          () -> callableStatement.getDate(1, Calendar.getInstance()));
      expectFeatureNotSupportedException(() -> callableStatement.getDouble("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getDouble(1));
      expectFeatureNotSupportedException(() -> callableStatement.getFloat("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getFloat(1));
      expectFeatureNotSupportedException(() -> callableStatement.getInt("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getInt(1));
      expectFeatureNotSupportedException(() -> callableStatement.getLong("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getLong(1));
      expectFeatureNotSupportedException(() -> callableStatement.getNCharacterStream("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getNCharacterStream(1));
      expectFeatureNotSupportedException(() -> callableStatement.getNClob("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getNClob(1));
      expectFeatureNotSupportedException(() -> callableStatement.getNString("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getNString(1));
      expectFeatureNotSupportedException(() -> callableStatement.getObject("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getObject(1));
      expectFeatureNotSupportedException(() -> callableStatement.getObject(1, String.class));
      expectFeatureNotSupportedException(() -> callableStatement.getObject(1, new HashMap<>()));
      expectFeatureNotSupportedException(() -> callableStatement.getObject("param_name"));
      expectFeatureNotSupportedException(
          () -> callableStatement.getObject("param_name", String.class));
      expectFeatureNotSupportedException(
          () -> callableStatement.getObject("param_name", new HashMap<>()));
      expectFeatureNotSupportedException(() -> callableStatement.getRef("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getRef(1));
      expectFeatureNotSupportedException(() -> callableStatement.getRowId("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getRowId(1));
      expectFeatureNotSupportedException(() -> callableStatement.getShort("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getShort(1));
      expectFeatureNotSupportedException(() -> callableStatement.getSQLXML("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getSQLXML(1));
      expectFeatureNotSupportedException(() -> callableStatement.getString("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getString(1));
      expectFeatureNotSupportedException(() -> callableStatement.getTime("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getTime(1));
      expectFeatureNotSupportedException(
          () -> callableStatement.getTime("param_name", Calendar.getInstance()));
      expectFeatureNotSupportedException(
          () -> callableStatement.getTime(1, Calendar.getInstance()));
      expectFeatureNotSupportedException(() -> callableStatement.getTimestamp("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getTimestamp(1));
      expectFeatureNotSupportedException(
          () -> callableStatement.getTimestamp("param_name", Calendar.getInstance()));
      expectFeatureNotSupportedException(
          () -> callableStatement.getTimestamp(1, Calendar.getInstance()));
      expectFeatureNotSupportedException(() -> callableStatement.getURL("param_name"));
      expectFeatureNotSupportedException(() -> callableStatement.getURL(1));
      expectFeatureNotSupportedException(
          () -> callableStatement.setAsciiStream("param_name", new FakeInputStream()));
      expectFeatureNotSupportedException(
          () -> callableStatement.setAsciiStream("param_name", new FakeInputStream(), 1));
      expectFeatureNotSupportedException(
          () -> callableStatement.setBigDecimal("param_name", BigDecimal.ONE));
      expectFeatureNotSupportedException(
          () -> callableStatement.setBinaryStream("param_name", new FakeInputStream()));
      expectFeatureNotSupportedException(
          () -> callableStatement.setBinaryStream("param_name", new FakeInputStream(), 1));
      expectFeatureNotSupportedException(
          () -> callableStatement.setBinaryStream("param_name", new FakeInputStream(), 5213L));
      expectFeatureNotSupportedException(
          () -> callableStatement.setBlob("param_name", new FakeInputStream()));
      expectFeatureNotSupportedException(
          () -> callableStatement.setBlob("param_name", new FakeInputStream(), 1L));
      expectFeatureNotSupportedException(
          () -> callableStatement.setBlob("param_name", new FakeBlob()));
      expectFeatureNotSupportedException(() -> callableStatement.setBoolean("param_name", true));
      expectFeatureNotSupportedException(() -> callableStatement.setByte("param_name", (byte) 6));
      expectFeatureNotSupportedException(
          () -> callableStatement.setBytes("param_name", "bytes".getBytes()));
      expectFeatureNotSupportedException(
          () -> callableStatement.setCharacterStream("param_name", new FakeReader()));
      expectFeatureNotSupportedException(
          () -> callableStatement.setCharacterStream("param_name", new FakeReader(), 1));
      expectFeatureNotSupportedException(
          () -> callableStatement.setCharacterStream("param_name", new FakeReader(), 1L));
      expectFeatureNotSupportedException(
          () -> callableStatement.setClob("param_name", new FakeReader()));
      expectFeatureNotSupportedException(
          () -> callableStatement.setClob("param_name", new FakeReader(), 1));
      expectFeatureNotSupportedException(
          () -> callableStatement.setClob("param_name", new FakeNClob()));
      expectFeatureNotSupportedException(
          () -> callableStatement.setDate("param_name", Date.valueOf("2019-07-07")));
      expectFeatureNotSupportedException(
          () ->
              callableStatement.setDate(
                  "param_name", Date.valueOf("2019-07-07"), Calendar.getInstance()));
      expectFeatureNotSupportedException(() -> callableStatement.setDouble("param_name", 3.0));
      expectFeatureNotSupportedException(() -> callableStatement.setFloat("param_name", 3.0f));
      expectFeatureNotSupportedException(() -> callableStatement.setInt("param_name", 3));
      expectFeatureNotSupportedException(() -> callableStatement.setLong("param_name", 3L));
      expectFeatureNotSupportedException(
          () -> callableStatement.setNCharacterStream("param_name", new FakeReader()));
      expectFeatureNotSupportedException(
          () -> callableStatement.setNCharacterStream("param_name", new FakeReader(), 1L));
      expectFeatureNotSupportedException(
          () -> callableStatement.setNClob("param_name", new FakeNClob()));
      expectFeatureNotSupportedException(
          () -> callableStatement.setNClob("param_name", new FakeReader(), 1));
      expectFeatureNotSupportedException(
          () -> callableStatement.setNClob("param_name", new FakeReader()));
      expectFeatureNotSupportedException(() -> callableStatement.setNString("param_name", "test"));
      expectFeatureNotSupportedException(() -> callableStatement.setNull("param_name", Types.NULL));
      expectFeatureNotSupportedException(
          () -> callableStatement.setNull("param_name", Types.NULL, "null"));
      expectFeatureNotSupportedException(
          () -> callableStatement.setObject("param_name", new Object()));
      expectFeatureNotSupportedException(
          () -> callableStatement.setObject("param_name", new Object(), Types.JAVA_OBJECT));
      expectFeatureNotSupportedException(
          () -> callableStatement.setObject("param_name", new Object(), Types.JAVA_OBJECT, 2));
      expectFeatureNotSupportedException(
          () -> callableStatement.setRowId("param_name", new FakeRowId()));
      expectFeatureNotSupportedException(() -> callableStatement.setShort("param_name", (short) 1));
      expectFeatureNotSupportedException(
          () -> callableStatement.setSQLXML("param_name", new FakeSQLXML()));
      expectFeatureNotSupportedException(() -> callableStatement.setString("param_name", "test"));
      expectFeatureNotSupportedException(
          () -> callableStatement.setTime("param_name", new Time(50)));
      expectFeatureNotSupportedException(
          () -> callableStatement.setTime("param_name", new Time(50), Calendar.getInstance()));
      expectFeatureNotSupportedException(
          () -> callableStatement.setTimestamp("param_name", new Timestamp(50)));
      expectFeatureNotSupportedException(
          () ->
              callableStatement.setTimestamp(
                  "param_name", new Timestamp(50), Calendar.getInstance()));
      URL fakeURL = new URL("http://localhost:8888/");
      expectFeatureNotSupportedException(() -> callableStatement.setURL(1, fakeURL));
      expectFeatureNotSupportedException(() -> callableStatement.wasNull());
    }
  }
}
