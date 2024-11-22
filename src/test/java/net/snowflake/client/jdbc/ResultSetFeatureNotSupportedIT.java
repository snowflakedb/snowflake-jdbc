package net.snowflake.client.jdbc;

import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.RESULT_SET)
public class ResultSetFeatureNotSupportedIT extends BaseJDBCWithSharedConnectionIT {
  @Test
  public void testQueryResultSetNotSupportedException() throws Throwable {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1")) {
      checkFeatureNotSupportedException(resultSet);
    }
  }

  @Test
  public void testMetadataResultSetNotSupportedException() throws Throwable {
    DatabaseMetaData metaData = connection.getMetaData();
    String database = connection.getCatalog();
    String schema = connection.getSchema();

    checkFeatureNotSupportedException(metaData.getCatalogs());
    checkFeatureNotSupportedException(metaData.getSchemas());
    checkFeatureNotSupportedException(metaData.getSchemas(database, null));
    checkFeatureNotSupportedException(metaData.getTables(database, schema, null, null));
    checkFeatureNotSupportedException(metaData.getColumns(database, schema, null, null));
  }

  private void checkFeatureNotSupportedException(ResultSet resultSet) throws SQLException {
    expectFeatureNotSupportedException(() -> resultSet.getAsciiStream(1));
    expectFeatureNotSupportedException(() -> resultSet.getBinaryStream(1));

    expectFeatureNotSupportedException(() -> resultSet.getAsciiStream("col1"));
    expectFeatureNotSupportedException(() -> resultSet.getBinaryStream("col2"));

    expectFeatureNotSupportedException(resultSet::getCursorName);
    expectFeatureNotSupportedException(resultSet::beforeFirst);
    expectFeatureNotSupportedException(resultSet::afterLast);
    expectFeatureNotSupportedException(resultSet::first);
    expectFeatureNotSupportedException(resultSet::last);
    expectFeatureNotSupportedException(resultSet::previous);

    expectFeatureNotSupportedException(() -> resultSet.absolute(0));
    expectFeatureNotSupportedException(() -> resultSet.relative(0));
    expectFeatureNotSupportedException(() -> resultSet.relative(0));

    expectFeatureNotSupportedException(resultSet::rowUpdated);
    expectFeatureNotSupportedException(resultSet::rowInserted);
    expectFeatureNotSupportedException(resultSet::rowDeleted);
    expectFeatureNotSupportedException(resultSet::rowUpdated);

    expectFeatureNotSupportedException(() -> resultSet.updateNull(1));
    expectFeatureNotSupportedException(() -> resultSet.updateBoolean(1, true));
    expectFeatureNotSupportedException(() -> resultSet.updateByte(1, (byte) 1));
    expectFeatureNotSupportedException(() -> resultSet.updateShort(1, (short) 2));
    expectFeatureNotSupportedException(() -> resultSet.updateInt(1, 3));
    expectFeatureNotSupportedException(() -> resultSet.updateLong(1, 4L));
    expectFeatureNotSupportedException(() -> resultSet.updateFloat(1, (float) 5.0));
    expectFeatureNotSupportedException(() -> resultSet.updateDouble(1, 6.0));
    expectFeatureNotSupportedException(() -> resultSet.updateBigDecimal(1, new BigDecimal(7)));
    expectFeatureNotSupportedException(() -> resultSet.updateString(1, "test1"));
    expectFeatureNotSupportedException(() -> resultSet.updateBytes(1, new byte[] {}));
    expectFeatureNotSupportedException(() -> resultSet.updateDate(1, new Date(1)));
    expectFeatureNotSupportedException(() -> resultSet.updateTime(1, new Time(0)));
    expectFeatureNotSupportedException(() -> resultSet.updateTimestamp(1, new Timestamp(3)));
    expectFeatureNotSupportedException(() -> resultSet.updateAsciiStream(1, null));
    expectFeatureNotSupportedException(() -> resultSet.updateBinaryStream(1, null));
    expectFeatureNotSupportedException(() -> resultSet.updateCharacterStream(1, null));
    expectFeatureNotSupportedException(() -> resultSet.updateAsciiStream(1, null));
    expectFeatureNotSupportedException(() -> resultSet.updateObject(1, new Object(), 124));
    expectFeatureNotSupportedException(() -> resultSet.updateObject(1, new Object()));

    expectFeatureNotSupportedException(() -> resultSet.updateNull("col1"));
    expectFeatureNotSupportedException(() -> resultSet.updateBoolean("col1", true));
    expectFeatureNotSupportedException(() -> resultSet.updateByte("col1", (byte) 1));
    expectFeatureNotSupportedException(() -> resultSet.updateShort("col1", (short) 2));
    expectFeatureNotSupportedException(() -> resultSet.updateInt("col1", 3));
    expectFeatureNotSupportedException(() -> resultSet.updateLong("col1", 4L));
    expectFeatureNotSupportedException(() -> resultSet.updateFloat("col1", (float) 5.0));
    expectFeatureNotSupportedException(() -> resultSet.updateDouble("col1", 6.0));
    expectFeatureNotSupportedException(() -> resultSet.updateBigDecimal("col1", new BigDecimal(7)));
    expectFeatureNotSupportedException(() -> resultSet.updateString("col1", "test1"));
    expectFeatureNotSupportedException(() -> resultSet.updateBytes("col1", new byte[] {}));
    expectFeatureNotSupportedException(() -> resultSet.updateDate("col1", new Date(0)));
    expectFeatureNotSupportedException(() -> resultSet.updateTime("col1", new Time(0)));
    expectFeatureNotSupportedException(() -> resultSet.updateTimestamp("col1", new Timestamp(3)));
    expectFeatureNotSupportedException(() -> resultSet.updateAsciiStream("col1", null));
    expectFeatureNotSupportedException(() -> resultSet.updateBinaryStream("col1", null));
    expectFeatureNotSupportedException(() -> resultSet.updateCharacterStream("col1", null));
    expectFeatureNotSupportedException(() -> resultSet.updateAsciiStream("col1", null));
    expectFeatureNotSupportedException(() -> resultSet.updateObject("col1", new Object(), 124));
    expectFeatureNotSupportedException(() -> resultSet.updateObject("col1", new Object()));

    expectFeatureNotSupportedException(resultSet::insertRow);
    expectFeatureNotSupportedException(resultSet::updateRow);
    expectFeatureNotSupportedException(resultSet::deleteRow);
    expectFeatureNotSupportedException(resultSet::refreshRow);
    expectFeatureNotSupportedException(resultSet::cancelRowUpdates);
    expectFeatureNotSupportedException(resultSet::moveToInsertRow);
    expectFeatureNotSupportedException(resultSet::moveToCurrentRow);

    expectFeatureNotSupportedException(() -> resultSet.getObject(1, Collections.emptyMap()));
    expectFeatureNotSupportedException(() -> resultSet.getRef(1));
    expectFeatureNotSupportedException(() -> resultSet.getBlob(1));
    expectFeatureNotSupportedException(() -> resultSet.getURL(1));
    expectFeatureNotSupportedException(() -> resultSet.getRowId(1));
    expectFeatureNotSupportedException(() -> resultSet.getNClob(1));
    expectFeatureNotSupportedException(() -> resultSet.getSQLXML(1));
    expectFeatureNotSupportedException(() -> resultSet.getNString(1));
    expectFeatureNotSupportedException(() -> resultSet.getNCharacterStream(1));
    expectFeatureNotSupportedException(() -> resultSet.getNClob(1));

    expectFeatureNotSupportedException(() -> resultSet.updateRef(1, new FakeRef()));
    expectFeatureNotSupportedException(() -> resultSet.updateBlob(1, new FakeBlob()));
    expectFeatureNotSupportedException(() -> resultSet.updateClob(1, new SnowflakeClob()));
    expectFeatureNotSupportedException(() -> resultSet.updateArray(1, new FakeArray()));
    expectFeatureNotSupportedException(() -> resultSet.updateRowId(1, new FakeRowId()));
    expectFeatureNotSupportedException(() -> resultSet.updateNString(1, "testN"));
    expectFeatureNotSupportedException(() -> resultSet.updateNClob(1, new FakeNClob()));
    expectFeatureNotSupportedException(() -> resultSet.updateSQLXML(1, new FakeSQLXML()));
    expectFeatureNotSupportedException(
        () -> resultSet.updateNCharacterStream(1, new FakeReader(), 100));
    expectFeatureNotSupportedException(() -> resultSet.updateNCharacterStream(1, new FakeReader()));
    expectFeatureNotSupportedException(
        () -> resultSet.updateAsciiStream(1, new FakeInputStream(), 100));
    expectFeatureNotSupportedException(() -> resultSet.updateAsciiStream(1, new FakeInputStream()));
    expectFeatureNotSupportedException(
        () -> resultSet.updateBinaryStream(1, new FakeInputStream(), 100));
    expectFeatureNotSupportedException(
        () -> resultSet.updateBinaryStream(1, new FakeInputStream()));
    expectFeatureNotSupportedException(
        () -> resultSet.updateCharacterStream(1, new FakeReader(), 100));
    expectFeatureNotSupportedException(() -> resultSet.updateCharacterStream(1, new FakeReader()));
    expectFeatureNotSupportedException(() -> resultSet.updateBlob(1, new FakeInputStream(), 100));
    expectFeatureNotSupportedException(() -> resultSet.updateBlob(1, new FakeInputStream()));
    expectFeatureNotSupportedException(() -> resultSet.updateClob(1, new FakeReader(), 100));
    expectFeatureNotSupportedException(() -> resultSet.updateClob(1, new FakeReader()));
    expectFeatureNotSupportedException(() -> resultSet.updateNClob(1, new FakeReader(), 100));
    expectFeatureNotSupportedException(() -> resultSet.updateNClob(1, new FakeReader()));

    expectFeatureNotSupportedException(() -> resultSet.getObject("col1", Collections.emptyMap()));
    expectFeatureNotSupportedException(() -> resultSet.getRef("col1"));
    expectFeatureNotSupportedException(() -> resultSet.getBlob("col1"));
    expectFeatureNotSupportedException(() -> resultSet.getArray("col2"));
    expectFeatureNotSupportedException(() -> resultSet.getURL("col2"));
    expectFeatureNotSupportedException(() -> resultSet.getRowId("col2"));
    expectFeatureNotSupportedException(() -> resultSet.getNClob("col2"));
    expectFeatureNotSupportedException(() -> resultSet.getSQLXML("col2"));
    expectFeatureNotSupportedException(() -> resultSet.getNString("col2"));
    expectFeatureNotSupportedException(() -> resultSet.getNCharacterStream("col2"));
    expectFeatureNotSupportedException(() -> resultSet.getNClob("col2"));

    expectFeatureNotSupportedException(() -> resultSet.updateRef("col2", new FakeRef()));
    expectFeatureNotSupportedException(() -> resultSet.updateBlob("col2", new FakeBlob()));
    expectFeatureNotSupportedException(() -> resultSet.updateClob("col2", new SnowflakeClob()));
    expectFeatureNotSupportedException(() -> resultSet.updateArray("col2", new FakeArray()));
    expectFeatureNotSupportedException(() -> resultSet.updateRowId("col2", new FakeRowId()));
    expectFeatureNotSupportedException(() -> resultSet.updateNString("col2", "testN"));
    expectFeatureNotSupportedException(() -> resultSet.updateNClob("col2", new FakeNClob()));
    expectFeatureNotSupportedException(() -> resultSet.updateSQLXML("col2", new FakeSQLXML()));
    expectFeatureNotSupportedException(
        () -> resultSet.updateNCharacterStream("col2", new FakeReader(), 100));
    expectFeatureNotSupportedException(
        () -> resultSet.updateNCharacterStream("col2", new FakeReader()));
    expectFeatureNotSupportedException(
        () -> resultSet.updateAsciiStream("col2", new FakeInputStream(), 100));
    expectFeatureNotSupportedException(
        () -> resultSet.updateAsciiStream("col2", new FakeInputStream()));
    expectFeatureNotSupportedException(
        () -> resultSet.updateBinaryStream("col2", new FakeInputStream(), 100));
    expectFeatureNotSupportedException(
        () -> resultSet.updateBinaryStream("col2", new FakeInputStream()));
    expectFeatureNotSupportedException(
        () -> resultSet.updateCharacterStream("col2", new FakeReader(), 100));
    expectFeatureNotSupportedException(
        () -> resultSet.updateCharacterStream("col2", new FakeReader()));
    expectFeatureNotSupportedException(
        () -> resultSet.updateBlob("col2", new FakeInputStream(), 100));
    expectFeatureNotSupportedException(() -> resultSet.updateBlob("col2", new FakeInputStream()));
    expectFeatureNotSupportedException(() -> resultSet.updateClob("col2", new FakeReader(), 100));
    expectFeatureNotSupportedException(() -> resultSet.updateClob("col2", new FakeReader()));
    expectFeatureNotSupportedException(() -> resultSet.updateNClob("col2", new FakeReader(), 100));
    expectFeatureNotSupportedException(() -> resultSet.updateNClob("col2", new FakeReader()));
  }
}
