/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.TimeZone;
import net.snowflake.client.category.TestCategoryResultSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryResultSet.class)
public class ResultSetAlreadyClosedIT extends BaseJDBCTest {
  @Test
  public void testQueryResultSetAlreadyClosed() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("select null::vector(float, 2)")) {
        checkAlreadyClosed(resultSet);
      }
      try (ResultSet resultSet = statement.executeQuery("select 1")) {
        checkAlreadyClosedInSnowflakeBaseResultSet(resultSet);
      }
    }
  }

  @Test
  public void testMetadataResultSetAlreadyClosed() throws Throwable {
    try (Connection connection = getConnection()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      DatabaseMetaData metaData = connection.getMetaData();

      checkAlreadyClosedMetaData(metaData.getCatalogs());
      checkAlreadyClosedMetaData(metaData.getSchemas());
      checkAlreadyClosedMetaData(metaData.getSchemas(database, null));
      checkAlreadyClosedMetaData(metaData.getTables(database, schema, null, null));
      checkAlreadyClosedMetaData(metaData.getColumns(database, schema, null, null));
    }
  }

  @Test
  public void testEmptyResultSetAlreadyClosed() throws Throwable {
    try (ResultSet resultSet = new SnowflakeResultSetV1.EmptyResultSet()) {
      checkAlreadyClosedEmpty(resultSet);
    }
  }

  public void checkAlreadyClosedInSnowflakeBaseResultSet(ResultSet resultSet) throws SQLException {
    SnowflakeBaseResultSet rs = resultSet.unwrap(SnowflakeBaseResultSet.class);
    rs.close();
    rs.close(); // second close won't raise exception
    assertTrue(rs.isClosed());
    expectResultSetAlreadyClosedException(() -> rs.getDate(1));
    expectResultSetAlreadyClosedException(() -> rs.getTimestamp(1));
    expectResultSetAlreadyClosedException(rs::getWarnings);
    expectResultSetAlreadyClosedException(rs::clearWarnings);
    expectResultSetAlreadyClosedException(rs::getMetaData);
    expectResultSetAlreadyClosedException(() -> rs.getTimestamp("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getCharacterStream(1));
    expectResultSetAlreadyClosedException(rs::getFetchDirection);
    expectResultSetAlreadyClosedException(() -> rs.setFetchDirection(1));
    expectResultSetAlreadyClosedException(rs::getFetchSize);
    expectResultSetAlreadyClosedException(() -> rs.setFetchSize(1));
    expectResultSetAlreadyClosedException(rs::getType);
    expectResultSetAlreadyClosedException(rs::getConcurrency);
    expectResultSetAlreadyClosedException(rs::getHoldability);
  }

  public void checkAlreadyClosedMetaData(ResultSet resultSet) throws SQLException {
    SnowflakeDatabaseMetaDataResultSet metaDataResultSet =
        resultSet.unwrap(SnowflakeDatabaseMetaDataResultSet.class);
    metaDataResultSet.close();
    metaDataResultSet.close(); // second close won't raise exception
    assertTrue(metaDataResultSet.isClosed());
    assertFalse(metaDataResultSet.next()); // next after close should return false.

    expectResultSetAlreadyClosedException(metaDataResultSet::isFirst);
    expectResultSetAlreadyClosedException(metaDataResultSet::isBeforeFirst);
    expectResultSetAlreadyClosedException(metaDataResultSet::isLast);
    expectResultSetAlreadyClosedException(metaDataResultSet::isAfterLast);
    expectResultSetAlreadyClosedException(metaDataResultSet::getRow);
    expectResultSetAlreadyClosedException(() -> metaDataResultSet.getBytes(1));
    expectResultSetAlreadyClosedException(() -> metaDataResultSet.getTime(1));
    expectResultSetAlreadyClosedException(
        () -> metaDataResultSet.getTimestamp(1, TimeZone.getDefault()));
    expectResultSetAlreadyClosedException(
        () -> metaDataResultSet.getDate(1, TimeZone.getDefault()));
    expectResultSetAlreadyClosedException(
        () -> metaDataResultSet.getDate(1, TimeZone.getDefault()));
    expectResultSetAlreadyClosedException(metaDataResultSet::wasNull);
  }

  private void checkAlreadyClosed(ResultSet resultSet) throws SQLException {
    SnowflakeResultSetV1 rs = resultSet.unwrap(SnowflakeResultSetV1.class);

    rs.close();
    rs.close(); // second close won't raise exception
    assertTrue(rs.isClosed());
    assertFalse(rs.next()); // next after close should return false.

    expectResultSetAlreadyClosedException(resultSet::wasNull);
    expectResultSetAlreadyClosedException(() -> rs.getString(1));
    expectResultSetAlreadyClosedException(() -> rs.getBoolean(1));
    expectResultSetAlreadyClosedException(() -> rs.getByte(1));
    expectResultSetAlreadyClosedException(() -> rs.getShort(1));
    expectResultSetAlreadyClosedException(() -> rs.getInt(1));
    expectResultSetAlreadyClosedException(() -> rs.getLong(1));
    expectResultSetAlreadyClosedException(() -> rs.getFloat(1));
    expectResultSetAlreadyClosedException(() -> rs.getDouble(1));

    expectResultSetAlreadyClosedException(() -> rs.getDate(1, TimeZone.getDefault()));
    expectResultSetAlreadyClosedException(() -> rs.getTime(1));
    expectResultSetAlreadyClosedException(() -> rs.getTimestamp(1, TimeZone.getDefault()));

    expectResultSetAlreadyClosedException(resultSet::getMetaData);
    expectResultSetAlreadyClosedException(() -> rs.getObject(1));
    expectResultSetAlreadyClosedException(() -> rs.getArray(1));

    expectResultSetAlreadyClosedException(() -> rs.getBigDecimal(1));
    expectResultSetAlreadyClosedException(() -> rs.getBigDecimal(1, 1));

    expectResultSetAlreadyClosedException(() -> rs.getBytes(1));

    expectResultSetAlreadyClosedException(rs::getRow);
    expectResultSetAlreadyClosedException(rs::isFirst);
    expectResultSetAlreadyClosedException(rs::isLast);
    expectResultSetAlreadyClosedException(rs::isAfterLast);
    expectResultSetAlreadyClosedException(rs::isBeforeFirst);

    expectResultSetAlreadyClosedException(() -> rs.getResultSetSerializables(1));
  }

  /**
   * These tests are specific to an empty resultset object
   *
   * @param rs
   * @throws SQLException
   */
  private void checkAlreadyClosedEmpty(ResultSet rs) throws SQLException {
    rs.close();
    rs.close();
    assertTrue(rs.isClosed());
    assertFalse(rs.next()); // next after close should return false.

    expectResultSetAlreadyClosedException(rs::wasNull);
    expectResultSetAlreadyClosedException(() -> rs.getBoolean(1));
    expectResultSetAlreadyClosedException(() -> rs.getInt(1));
    expectResultSetAlreadyClosedException(() -> rs.getLong(1));
    expectResultSetAlreadyClosedException(() -> rs.getFloat(1));
    expectResultSetAlreadyClosedException(() -> rs.getDouble(1));
    expectResultSetAlreadyClosedException(() -> rs.getShort(1));
    expectResultSetAlreadyClosedException(() -> rs.getByte(1));
    expectResultSetAlreadyClosedException(() -> rs.getString(1));
    expectResultSetAlreadyClosedException(() -> rs.getBytes(1));
    expectResultSetAlreadyClosedException(() -> rs.getBytes(1));
    expectResultSetAlreadyClosedException(() -> rs.getDate(1));
    expectResultSetAlreadyClosedException(() -> rs.getTime(1));
    expectResultSetAlreadyClosedException(() -> rs.getTimestamp(1));
    expectResultSetAlreadyClosedException(() -> rs.getAsciiStream(1));
    expectResultSetAlreadyClosedException(() -> rs.getUnicodeStream(1));
    expectResultSetAlreadyClosedException(() -> rs.getBinaryStream(1));

    expectResultSetAlreadyClosedException(() -> rs.getString("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getBoolean("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getByte("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getShort("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getInt("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getLong("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getFloat("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getDouble("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getBigDecimal("col1", 1));
    expectResultSetAlreadyClosedException(() -> rs.getBytes("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getDate("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getTime("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getTimestamp("col1"));

    expectResultSetAlreadyClosedException(() -> rs.getBigDecimal(1));
    expectResultSetAlreadyClosedException(() -> rs.getBigDecimal("col1"));

    expectResultSetAlreadyClosedException(rs::isBeforeFirst);
    expectResultSetAlreadyClosedException(rs::isAfterLast);
    expectResultSetAlreadyClosedException(rs::isFirst);
    expectResultSetAlreadyClosedException(rs::isLast);
    expectResultSetAlreadyClosedException(rs::beforeFirst);
    expectResultSetAlreadyClosedException(rs::afterLast);
    expectResultSetAlreadyClosedException(rs::first);
    expectResultSetAlreadyClosedException(rs::last);
    expectResultSetAlreadyClosedException(rs::getRow);

    expectResultSetAlreadyClosedException(() -> rs.absolute(1));
    expectResultSetAlreadyClosedException(() -> rs.relative(1));

    expectResultSetAlreadyClosedException(rs::previous);

    expectResultSetAlreadyClosedException(() -> rs.setFetchDirection(1));
    expectResultSetAlreadyClosedException(rs::getFetchDirection);
    expectResultSetAlreadyClosedException(() -> rs.setFetchSize(1));
    expectResultSetAlreadyClosedException(rs::getFetchSize);
    expectResultSetAlreadyClosedException(rs::getType);
    expectResultSetAlreadyClosedException(rs::getConcurrency);
    expectResultSetAlreadyClosedException(rs::rowUpdated);
    expectResultSetAlreadyClosedException(rs::rowInserted);
    expectResultSetAlreadyClosedException(rs::rowDeleted);

    expectResultSetAlreadyClosedException(() -> rs.updateNull(1));
    expectResultSetAlreadyClosedException(() -> rs.updateBoolean(1, true));
    expectResultSetAlreadyClosedException(() -> rs.updateByte(1, (byte) 0));
    expectResultSetAlreadyClosedException(() -> rs.updateShort(1, (short) 1));
    expectResultSetAlreadyClosedException(() -> rs.updateInt(1, 1));
    expectResultSetAlreadyClosedException(() -> rs.updateLong(1, 1L));
    expectResultSetAlreadyClosedException(() -> rs.updateFloat(1, 1.0f));
    expectResultSetAlreadyClosedException(() -> rs.updateDouble(1, 1.0));
    expectResultSetAlreadyClosedException(() -> rs.updateBigDecimal(1, BigDecimal.ONE));
    expectResultSetAlreadyClosedException(() -> rs.updateString(1, "a"));
    expectResultSetAlreadyClosedException(() -> rs.updateBytes(1, new byte[] {}));

    expectResultSetAlreadyClosedException(
        () -> rs.updateDate(1, new Date(System.currentTimeMillis())));
    expectResultSetAlreadyClosedException(
        () -> rs.updateTime(1, new Time(System.currentTimeMillis())));
    expectResultSetAlreadyClosedException(
        () -> rs.updateTimestamp(1, new Timestamp(System.currentTimeMillis())));

    expectResultSetAlreadyClosedException(() -> rs.updateAsciiStream(1, new FakeInputStream(), 1));
    expectResultSetAlreadyClosedException(() -> rs.updateBinaryStream(1, new FakeInputStream(), 1));
    expectResultSetAlreadyClosedException(() -> rs.updateCharacterStream(1, new FakeReader(), 1));
    expectResultSetAlreadyClosedException(() -> rs.updateObject(1, new Object(), 1));
    expectResultSetAlreadyClosedException(() -> rs.updateObject(1, new Object()));

    expectResultSetAlreadyClosedException(() -> rs.updateNull("col1"));
    expectResultSetAlreadyClosedException(() -> rs.updateBoolean("col1", false));
    expectResultSetAlreadyClosedException(() -> rs.updateByte("col1", (byte) 0));
    expectResultSetAlreadyClosedException(() -> rs.updateShort("col1", (short) 1));
    expectResultSetAlreadyClosedException(() -> rs.updateInt("col1", 1));
    expectResultSetAlreadyClosedException(() -> rs.updateLong("col1", 1L));
    expectResultSetAlreadyClosedException(() -> rs.updateFloat("col1", 1f));
    expectResultSetAlreadyClosedException(() -> rs.updateDouble("col1", 1.0));
    expectResultSetAlreadyClosedException(() -> rs.updateBigDecimal("col1", BigDecimal.ONE));
    expectResultSetAlreadyClosedException(() -> rs.updateString("col1", "a"));
    expectResultSetAlreadyClosedException(() -> rs.updateBytes("col1", new byte[] {}));
    expectResultSetAlreadyClosedException(
        () -> rs.updateDate("col1", new Date(System.currentTimeMillis())));
    expectResultSetAlreadyClosedException(
        () -> rs.updateTime("col1", new Time(System.currentTimeMillis())));
    expectResultSetAlreadyClosedException(
        () -> rs.updateTimestamp("col1", new Timestamp(System.currentTimeMillis())));
    expectResultSetAlreadyClosedException(
        () -> rs.updateAsciiStream("col1", new FakeInputStream(), 1));
    expectResultSetAlreadyClosedException(
        () -> rs.updateBinaryStream("col1", new FakeInputStream(), 1));
    expectResultSetAlreadyClosedException(
        () -> rs.updateCharacterStream("col1", new FakeReader(), 1));
    expectResultSetAlreadyClosedException(() -> rs.updateObject("col1", new Object(), 1));
    expectResultSetAlreadyClosedException(() -> rs.updateObject("col1", new Object()));

    expectResultSetAlreadyClosedException(rs::insertRow);
    expectResultSetAlreadyClosedException(rs::updateRow);
    expectResultSetAlreadyClosedException(rs::deleteRow);
    expectResultSetAlreadyClosedException(rs::refreshRow);
    expectResultSetAlreadyClosedException(rs::cancelRowUpdates);
    expectResultSetAlreadyClosedException(rs::moveToInsertRow);
    expectResultSetAlreadyClosedException(rs::moveToCurrentRow);
    expectResultSetAlreadyClosedException(rs::getStatement);

    expectResultSetAlreadyClosedException(() -> rs.getObject(1, new HashMap<>()));
    expectResultSetAlreadyClosedException(() -> rs.getRef(1));
    expectResultSetAlreadyClosedException(() -> rs.getBlob(1));
    expectResultSetAlreadyClosedException(() -> rs.getClob(1));
    expectResultSetAlreadyClosedException(() -> rs.getArray(1));
    expectResultSetAlreadyClosedException(() -> rs.getArray(1));
    expectResultSetAlreadyClosedException(() -> rs.getObject("col1", new HashMap<>()));
    expectResultSetAlreadyClosedException(() -> rs.getRef("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getBlob("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getClob("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getArray("col1"));

    expectResultSetAlreadyClosedException(() -> rs.getDate(1, new FakeCalendar()));
    expectResultSetAlreadyClosedException(() -> rs.getDate("col1", new FakeCalendar()));
    expectResultSetAlreadyClosedException(() -> rs.getTime(1, new FakeCalendar()));
    expectResultSetAlreadyClosedException(() -> rs.getTime("col1", new FakeCalendar()));
    expectResultSetAlreadyClosedException(() -> rs.getTimestamp(1, new FakeCalendar()));
    expectResultSetAlreadyClosedException(() -> rs.getTimestamp("col1", new FakeCalendar()));
    expectResultSetAlreadyClosedException(() -> rs.getURL(1));
    expectResultSetAlreadyClosedException(() -> rs.getURL("col1"));
    expectResultSetAlreadyClosedException(() -> rs.updateRef(1, new FakeRef()));
    expectResultSetAlreadyClosedException(() -> rs.updateRef("col1", new FakeRef()));
    expectResultSetAlreadyClosedException(() -> rs.updateBlob(1, new FakeBlob()));
    expectResultSetAlreadyClosedException(() -> rs.updateBlob("col1", new FakeBlob()));
    expectResultSetAlreadyClosedException(() -> rs.updateClob(1, new FakeNClob()));
    expectResultSetAlreadyClosedException(() -> rs.updateClob("col1", new FakeNClob()));
    expectResultSetAlreadyClosedException(() -> rs.updateArray(1, new FakeArray()));
    expectResultSetAlreadyClosedException(() -> rs.updateArray("col1", new FakeArray()));

    expectResultSetAlreadyClosedException(() -> rs.getRowId(1));
    expectResultSetAlreadyClosedException(() -> rs.getRowId("col1"));
    expectResultSetAlreadyClosedException(() -> rs.updateRowId(1, new FakeRowId()));
    expectResultSetAlreadyClosedException(() -> rs.updateRowId("col1", new FakeRowId()));

    expectResultSetAlreadyClosedException(rs::getHoldability);

    expectResultSetAlreadyClosedException(() -> rs.updateNString(1, "a"));
    expectResultSetAlreadyClosedException(() -> rs.updateNString("col1", "a"));
    expectResultSetAlreadyClosedException(() -> rs.updateNClob(1, new FakeNClob()));
    expectResultSetAlreadyClosedException(() -> rs.updateNClob("col1", new FakeNClob()));
    expectResultSetAlreadyClosedException(() -> rs.updateNClob("col1", new FakeNClob()));
    expectResultSetAlreadyClosedException(() -> rs.getNClob(1));
    expectResultSetAlreadyClosedException(() -> rs.getNClob("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getSQLXML(1));
    expectResultSetAlreadyClosedException(() -> rs.getSQLXML("col1"));
    expectResultSetAlreadyClosedException(() -> rs.updateSQLXML(1, new FakeSQLXML()));
    expectResultSetAlreadyClosedException(() -> rs.updateSQLXML("col1", new FakeSQLXML()));

    expectResultSetAlreadyClosedException(() -> rs.getNString(1));
    expectResultSetAlreadyClosedException(() -> rs.getNString("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getNCharacterStream(1));
    expectResultSetAlreadyClosedException(() -> rs.getNCharacterStream("col1"));
    expectResultSetAlreadyClosedException(() -> rs.updateNCharacterStream(1, new FakeReader(), 1L));
    expectResultSetAlreadyClosedException(
        () -> rs.updateNCharacterStream("col1", new FakeReader(), 1L));
    expectResultSetAlreadyClosedException(() -> rs.updateAsciiStream(1, new FakeInputStream(), 1L));
    expectResultSetAlreadyClosedException(
        () -> rs.updateBinaryStream(1, new FakeInputStream(), 1L));
    expectResultSetAlreadyClosedException(() -> rs.updateCharacterStream(1, new FakeReader(), 1L));
    expectResultSetAlreadyClosedException(
        () -> rs.updateAsciiStream("col1", new FakeInputStream(), 1L));
    expectResultSetAlreadyClosedException(
        () -> rs.updateBinaryStream("col1", new FakeInputStream(), 1L));
    expectResultSetAlreadyClosedException(
        () -> rs.updateCharacterStream("col1", new FakeReader(), 1L));
    expectResultSetAlreadyClosedException(() -> rs.updateBlob(1, new FakeInputStream(), 1L));
    expectResultSetAlreadyClosedException(() -> rs.updateBlob("col1", new FakeInputStream(), 1L));
    expectResultSetAlreadyClosedException(() -> rs.updateClob(1, new FakeReader(), 1L));
    expectResultSetAlreadyClosedException(() -> rs.updateClob("col1", new FakeReader(), 1L));
    expectResultSetAlreadyClosedException(() -> rs.updateNClob(1, new FakeReader(), 1L));
    expectResultSetAlreadyClosedException(() -> rs.updateNClob("col1", new FakeReader(), 1L));

    expectResultSetAlreadyClosedException(() -> rs.updateNCharacterStream(1, new FakeReader()));
    expectResultSetAlreadyClosedException(
        () -> rs.updateNCharacterStream("col1", new FakeReader()));
    expectResultSetAlreadyClosedException(() -> rs.updateAsciiStream(1, new FakeInputStream()));
    expectResultSetAlreadyClosedException(() -> rs.updateBinaryStream(1, new FakeInputStream()));
    expectResultSetAlreadyClosedException(() -> rs.updateCharacterStream(1, new FakeReader()));
    expectResultSetAlreadyClosedException(
        () -> rs.updateAsciiStream("col1", new FakeInputStream()));
    expectResultSetAlreadyClosedException(
        () -> rs.updateBinaryStream("col1", new FakeInputStream()));
    expectResultSetAlreadyClosedException(() -> rs.updateCharacterStream("col1", new FakeReader()));
    expectResultSetAlreadyClosedException(() -> rs.updateBlob(1, new FakeInputStream()));
    expectResultSetAlreadyClosedException(() -> rs.updateBlob("col1", new FakeInputStream()));
    expectResultSetAlreadyClosedException(() -> rs.updateClob(1, new FakeReader()));
    expectResultSetAlreadyClosedException(() -> rs.updateClob("col1", new FakeReader()));
    expectResultSetAlreadyClosedException(() -> rs.updateNClob(1, new FakeReader()));
    expectResultSetAlreadyClosedException(() -> rs.updateNClob("col1", new FakeReader()));

    expectResultSetAlreadyClosedException(() -> rs.getObject(1, SnowflakeResultSetV1.class));
    expectResultSetAlreadyClosedException(() -> rs.getObject("col1", SnowflakeResultSetV1.class));
    expectResultSetAlreadyClosedException(() -> rs.getBigDecimal(1, 1));

    expectResultSetAlreadyClosedException(() -> rs.getAsciiStream("a"));
    expectResultSetAlreadyClosedException(() -> rs.getUnicodeStream("a"));
    expectResultSetAlreadyClosedException(() -> rs.getBinaryStream("a"));

    expectResultSetAlreadyClosedException(rs::getWarnings);
    expectResultSetAlreadyClosedException(rs::clearWarnings);
    expectResultSetAlreadyClosedException(rs::getCursorName);
    expectResultSetAlreadyClosedException(rs::getMetaData);

    expectResultSetAlreadyClosedException(() -> rs.getObject(1));
    expectResultSetAlreadyClosedException(() -> rs.getObject("col1"));
    expectResultSetAlreadyClosedException(() -> rs.findColumn("col1"));
    expectResultSetAlreadyClosedException(() -> rs.getCharacterStream(1));
    expectResultSetAlreadyClosedException(() -> rs.getCharacterStream("col1"));
    expectResultSetAlreadyClosedException(() -> rs.unwrap(SnowflakeResultSetV1.class));
    expectResultSetAlreadyClosedException(() -> rs.isWrapperFor(SnowflakeResultSetV1.class));
  }
}
