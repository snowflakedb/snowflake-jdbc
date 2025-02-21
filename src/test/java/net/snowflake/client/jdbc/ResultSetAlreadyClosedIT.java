package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.RESULT_SET)
public class ResultSetAlreadyClosedIT extends BaseJDBCWithSharedConnectionIT {

  @Test
  public void testQueryResultSetAlreadyClosed() throws Throwable {
    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("select 1");
      resultSet.close();
      checkAlreadyClosed(resultSet);
    }
  }

  @Test
  public void testMetadataResultSetAlreadyClosed() throws Throwable {
    String database = connection.getCatalog();
    String schema = connection.getSchema();
    DatabaseMetaData metaData = connection.getMetaData();

    checkAlreadyClosed(metaData.getCatalogs());
    checkAlreadyClosed(metaData.getSchemas());
    checkAlreadyClosed(metaData.getSchemas(database, null));
    checkAlreadyClosed(metaData.getTables(database, schema, null, null));
    checkAlreadyClosed(metaData.getColumns(database, schema, null, null));
  }

  @Test
  public void testResultSetAlreadyClosed() throws Throwable {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT 1")) {
      checkAlreadyClosed(resultSet);
    }
  }

  @Test
  public void testEmptyResultSetAlreadyClosed() throws Throwable {
    try (SnowflakeResultSetV1.EmptyResultSet resultSet =
        new SnowflakeResultSetV1.EmptyResultSet()) {
      checkAlreadyClosedEmpty(resultSet);
    }
  }

  private void checkAlreadyClosed(ResultSet resultSet) throws SQLException {
    resultSet.close();
    resultSet.close(); // second close won't raise exception
    assertTrue(resultSet.isClosed());
    assertFalse(resultSet.next()); // next after close should return false.

    expectResultSetAlreadyClosedException(resultSet::wasNull);
    expectResultSetAlreadyClosedException(() -> resultSet.getString(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getBoolean(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getByte(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getShort(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getInt(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getLong(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getFloat(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getDouble(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getBigDecimal(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getBytes(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getDate(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getTime(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getTimestamp(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getObject(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getCharacterStream(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getClob(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getDate(1, Calendar.getInstance()));
    expectResultSetAlreadyClosedException(() -> resultSet.getTime(1, Calendar.getInstance()));
    expectResultSetAlreadyClosedException(() -> resultSet.getTimestamp(1, Calendar.getInstance()));

    expectResultSetAlreadyClosedException(() -> resultSet.getString("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getBoolean("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getByte("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getShort("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getInt("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getLong("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getFloat("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getDouble("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getBigDecimal("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getBytes("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getString("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getDate("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getTime("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getTimestamp("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getObject("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getCharacterStream("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getClob("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.getDate("col1", Calendar.getInstance()));
    expectResultSetAlreadyClosedException(() -> resultSet.getTime("col1", Calendar.getInstance()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.getTimestamp("col1", Calendar.getInstance()));

    expectResultSetAlreadyClosedException(() -> resultSet.getBigDecimal(1, 28));
    expectResultSetAlreadyClosedException(() -> resultSet.getBigDecimal("col1", 38));

    expectResultSetAlreadyClosedException(resultSet::getWarnings);
    expectResultSetAlreadyClosedException(
        () -> resultSet.unwrap(SnowflakeBaseResultSet.class).getWarnings());

    expectResultSetAlreadyClosedException(resultSet::clearWarnings);
    expectResultSetAlreadyClosedException(
        () -> resultSet.unwrap(SnowflakeBaseResultSet.class).clearWarnings());

    expectResultSetAlreadyClosedException(resultSet::getMetaData);

    expectResultSetAlreadyClosedException(() -> resultSet.findColumn("col1"));

    expectResultSetAlreadyClosedException(resultSet::isBeforeFirst);
    expectResultSetAlreadyClosedException(resultSet::isAfterLast);
    expectResultSetAlreadyClosedException(resultSet::isFirst);
    expectResultSetAlreadyClosedException(resultSet::isLast);
    expectResultSetAlreadyClosedException(resultSet::getRow);

    expectResultSetAlreadyClosedException(
        () -> resultSet.setFetchDirection(ResultSet.FETCH_FORWARD));
    expectResultSetAlreadyClosedException(() -> resultSet.setFetchSize(10));
    expectResultSetAlreadyClosedException(
        () -> resultSet.unwrap(SnowflakeBaseResultSet.class).setFetchSize(10));

    expectResultSetAlreadyClosedException(resultSet::getFetchDirection);
    expectResultSetAlreadyClosedException(resultSet::getFetchSize);
    expectResultSetAlreadyClosedException(resultSet::getType);
    expectResultSetAlreadyClosedException(resultSet::getConcurrency);
    expectResultSetAlreadyClosedException(
        resultSet.unwrap(SnowflakeBaseResultSet.class)::getConcurrency);

    expectResultSetAlreadyClosedException(resultSet::getHoldability);
    expectResultSetAlreadyClosedException(
        resultSet.unwrap(SnowflakeBaseResultSet.class)::getHoldability);

    expectResultSetAlreadyClosedException(resultSet::getStatement);
  }

  /**
   * These tests are specific to an empty resultset object
   *
   * @param resultSet
   * @throws SQLException
   */
  private void checkAlreadyClosedEmpty(SnowflakeResultSetV1.EmptyResultSet resultSet)
      throws SQLException {
    resultSet.close();
    resultSet.close(); // second close won't raise exception
    assertTrue(resultSet.isClosed());
    assertFalse(resultSet.next()); // next after close should return false.

    expectResultSetAlreadyClosedException(resultSet::beforeFirst);
    expectResultSetAlreadyClosedException(resultSet::afterLast);
    expectResultSetAlreadyClosedException(resultSet::first);
    expectResultSetAlreadyClosedException(resultSet::last);
    expectResultSetAlreadyClosedException(resultSet::getRow);
    expectResultSetAlreadyClosedException(resultSet::previous);
    expectResultSetAlreadyClosedException(resultSet::rowUpdated);
    expectResultSetAlreadyClosedException(resultSet::rowInserted);
    expectResultSetAlreadyClosedException(resultSet::rowDeleted);

    expectResultSetAlreadyClosedException(() -> resultSet.absolute(1));
    expectResultSetAlreadyClosedException(() -> resultSet.relative(1));
    expectResultSetAlreadyClosedException(() -> resultSet.setFetchDirection(1));
    expectResultSetAlreadyClosedException(resultSet::getFetchDirection);
    expectResultSetAlreadyClosedException(() -> resultSet.setFetchSize(1));
    expectResultSetAlreadyClosedException(resultSet::getFetchSize);
    expectResultSetAlreadyClosedException(resultSet::getType);
    expectResultSetAlreadyClosedException(resultSet::getConcurrency);

    expectResultSetAlreadyClosedException(() -> resultSet.updateNull(1));
    expectResultSetAlreadyClosedException(() -> resultSet.updateNull("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.updateBoolean(2, true));
    expectResultSetAlreadyClosedException(() -> resultSet.updateBoolean("col2", true));
    expectResultSetAlreadyClosedException(() -> resultSet.updateByte(3, (byte) 0));
    expectResultSetAlreadyClosedException(() -> resultSet.updateByte("col3", (byte) 0));
    expectResultSetAlreadyClosedException(() -> resultSet.updateShort(4, (short) 0));
    expectResultSetAlreadyClosedException(() -> resultSet.updateShort("col4", (short) 0));
    expectResultSetAlreadyClosedException(() -> resultSet.updateInt(5, 0));
    expectResultSetAlreadyClosedException(() -> resultSet.updateInt("col5", 0));
    expectResultSetAlreadyClosedException(() -> resultSet.updateLong(6, 5L));
    expectResultSetAlreadyClosedException(() -> resultSet.updateLong("col6", 5L));
    expectResultSetAlreadyClosedException(() -> resultSet.updateFloat(6, 4F));
    expectResultSetAlreadyClosedException(() -> resultSet.updateFloat("col6", 4F));
    expectResultSetAlreadyClosedException(() -> resultSet.updateDouble(7, 12.5));
    expectResultSetAlreadyClosedException(() -> resultSet.updateDouble("col7", 12.5));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateBigDecimal(8, BigDecimal.valueOf(12.5)));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateBigDecimal("col8", BigDecimal.valueOf(12.5)));
    expectResultSetAlreadyClosedException(() -> resultSet.updateString(9, "hello"));
    expectResultSetAlreadyClosedException(() -> resultSet.updateString("col9", "hello"));
    expectResultSetAlreadyClosedException(() -> resultSet.updateBytes(10, new byte[0]));
    expectResultSetAlreadyClosedException(() -> resultSet.updateBytes("col10", new byte[0]));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateDate(11, new java.sql.Date(System.currentTimeMillis())));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateDate("col11", new java.sql.Date(System.currentTimeMillis())));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateTime(12, new java.sql.Time(System.currentTimeMillis())));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateTime("col12", new java.sql.Time(System.currentTimeMillis())));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateTimestamp(13, new java.sql.Timestamp(System.currentTimeMillis())));
    expectResultSetAlreadyClosedException(
        () ->
            resultSet.updateTimestamp("col13", new java.sql.Timestamp(System.currentTimeMillis())));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateAsciiStream(14, new FakeInputStream()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateAsciiStream(14, new FakeInputStream(), 5));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateAsciiStream(14, new FakeInputStream(), 5L));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateAsciiStream("col14", new FakeInputStream()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateAsciiStream("col14", new FakeInputStream(), 5));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateAsciiStream("col14", new FakeInputStream(), 5L));
    expectResultSetAlreadyClosedException(() -> resultSet.getAsciiStream(14));
    expectResultSetAlreadyClosedException(() -> resultSet.getAsciiStream("col14"));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateBinaryStream(15, new FakeInputStream()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateBinaryStream(15, new FakeInputStream(), 5));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateBinaryStream(15, new FakeInputStream(), 5L));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateBinaryStream("col15", new FakeInputStream()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateBinaryStream("col15", new FakeInputStream(), 5));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateBinaryStream("col15", new FakeInputStream(), 5L));
    expectResultSetAlreadyClosedException(() -> resultSet.getBinaryStream(15));
    expectResultSetAlreadyClosedException(() -> resultSet.getBinaryStream("col15"));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateCharacterStream(16, new FakeReader()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateCharacterStream(16, new FakeReader(), 5));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateCharacterStream(16, new FakeReader(), 5L));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateCharacterStream("col16", new FakeReader()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateCharacterStream("col16", new FakeReader(), 5));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateCharacterStream("col16", new FakeReader(), 5L));
    expectResultSetAlreadyClosedException(() -> resultSet.updateObject(17, new Object()));
    expectResultSetAlreadyClosedException(() -> resultSet.updateObject(17, new Object(), 5));
    expectResultSetAlreadyClosedException(() -> resultSet.updateObject("col17", new Object()));
    expectResultSetAlreadyClosedException(() -> resultSet.updateObject("col17", new Object(), 5));
    expectResultSetAlreadyClosedException(
        () -> resultSet.getObject(17, SnowflakeResultSetV1.class));
    expectResultSetAlreadyClosedException(
        () -> resultSet.getObject("col17", SnowflakeResultSetV1.class));
    expectResultSetAlreadyClosedException(
        () -> resultSet.getObject(17, SnowflakeResultSetV1.class));
    expectResultSetAlreadyClosedException(
        () -> resultSet.getObject("col17", SnowflakeResultSetV1.class));
    expectResultSetAlreadyClosedException(() -> resultSet.updateBlob(18, new FakeBlob()));
    expectResultSetAlreadyClosedException(() -> resultSet.updateBlob(18, new FakeInputStream()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateBlob(18, new FakeInputStream(), 5L));
    expectResultSetAlreadyClosedException(() -> resultSet.updateBlob("col18", new FakeBlob()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateBlob("col18", new FakeInputStream()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateBlob("col18", new FakeInputStream(), 5L));
    expectResultSetAlreadyClosedException(() -> resultSet.getBlob(18));
    expectResultSetAlreadyClosedException(() -> resultSet.getBlob("col18"));
    expectResultSetAlreadyClosedException(() -> resultSet.updateNull(19));
    expectResultSetAlreadyClosedException(() -> resultSet.updateNull("col19"));
    expectResultSetAlreadyClosedException(() -> resultSet.getUnicodeStream(20));
    expectResultSetAlreadyClosedException(() -> resultSet.getUnicodeStream("col20"));
    expectResultSetAlreadyClosedException(() -> resultSet.updateRef(21, new FakeRef()));
    expectResultSetAlreadyClosedException(() -> resultSet.updateRef("col21", new FakeRef()));
    expectResultSetAlreadyClosedException(() -> resultSet.getRef(21));
    expectResultSetAlreadyClosedException(() -> resultSet.getRef("col21"));
    expectResultSetAlreadyClosedException(() -> resultSet.updateArray(22, new FakeArray()));
    expectResultSetAlreadyClosedException(() -> resultSet.updateArray("col22", new FakeArray()));
    expectResultSetAlreadyClosedException(() -> resultSet.getArray(22));
    expectResultSetAlreadyClosedException(() -> resultSet.getArray("col22"));
    expectResultSetAlreadyClosedException(() -> resultSet.getURL(23));
    expectResultSetAlreadyClosedException(() -> resultSet.getURL("col23"));
    expectResultSetAlreadyClosedException(() -> resultSet.updateClob(24, new FakeNClob()));
    expectResultSetAlreadyClosedException(() -> resultSet.updateClob(24, new FakeReader(), 5L));
    expectResultSetAlreadyClosedException(() -> resultSet.updateClob(24, new FakeReader()));
    expectResultSetAlreadyClosedException(() -> resultSet.updateClob("col24", new FakeNClob()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateClob("col24", new FakeReader(), 5L));
    expectResultSetAlreadyClosedException(() -> resultSet.updateClob("col24", new FakeReader()));
    expectResultSetAlreadyClosedException(() -> resultSet.updateNString(25, "hello"));
    expectResultSetAlreadyClosedException(() -> resultSet.updateNString("col25", "hello"));
    expectResultSetAlreadyClosedException(() -> resultSet.getNString(25));
    expectResultSetAlreadyClosedException(() -> resultSet.getNString("col25"));
    expectResultSetAlreadyClosedException(() -> resultSet.updateNClob(26, new FakeNClob()));
    expectResultSetAlreadyClosedException(() -> resultSet.updateNClob(26, new FakeReader(), 5L));
    expectResultSetAlreadyClosedException(() -> resultSet.updateNClob(26, new FakeReader()));
    expectResultSetAlreadyClosedException(() -> resultSet.updateNClob("col26", new FakeNClob()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateNClob("col26", new FakeReader(), 5L));
    expectResultSetAlreadyClosedException(() -> resultSet.updateNClob("col26", new FakeReader()));
    expectResultSetAlreadyClosedException(() -> resultSet.getNClob(26));
    expectResultSetAlreadyClosedException(() -> resultSet.getNClob("col26"));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateNCharacterStream(27, new FakeReader()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateNCharacterStream(27, new FakeReader(), 5L));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateNCharacterStream("col26", new FakeReader()));
    expectResultSetAlreadyClosedException(
        () -> resultSet.updateNCharacterStream("col26", new FakeReader(), 5L));
    expectResultSetAlreadyClosedException(() -> resultSet.getNCharacterStream(26));
    expectResultSetAlreadyClosedException(() -> resultSet.getNCharacterStream("col26"));
    expectResultSetAlreadyClosedException(() -> resultSet.updateSQLXML(27, new FakeSQLXML()));
    expectResultSetAlreadyClosedException(() -> resultSet.updateSQLXML("col27", new FakeSQLXML()));
    expectResultSetAlreadyClosedException(() -> resultSet.getSQLXML(27));
    expectResultSetAlreadyClosedException(() -> resultSet.getSQLXML("col27"));

    expectResultSetAlreadyClosedException(() -> resultSet.getRowId(1));
    expectResultSetAlreadyClosedException(() -> resultSet.getRowId("col1"));
    expectResultSetAlreadyClosedException(() -> resultSet.updateRowId(1, new FakeRowId()));
    expectResultSetAlreadyClosedException(() -> resultSet.updateRowId("col1", new FakeRowId()));

    expectResultSetAlreadyClosedException(resultSet::insertRow);
    expectResultSetAlreadyClosedException(resultSet::updateRow);
    expectResultSetAlreadyClosedException(resultSet::deleteRow);
    expectResultSetAlreadyClosedException(resultSet::refreshRow);
    expectResultSetAlreadyClosedException(resultSet::cancelRowUpdates);
    expectResultSetAlreadyClosedException(resultSet::moveToInsertRow);
    expectResultSetAlreadyClosedException(resultSet::moveToCurrentRow);
    expectResultSetAlreadyClosedException(resultSet::cancelRowUpdates);

    expectResultSetAlreadyClosedException(() -> resultSet.isWrapperFor(SnowflakeResultSetV1.class));
    expectResultSetAlreadyClosedException(() -> resultSet.unwrap(SnowflakeResultSetV1.class));
  }
}
