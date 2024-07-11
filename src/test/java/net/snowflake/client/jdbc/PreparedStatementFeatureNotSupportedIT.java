/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import net.snowflake.client.category.TestCategoryStatement;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryStatement.class)
public class PreparedStatementFeatureNotSupportedIT extends BaseJDBCTest {
  @Test
  public void testFeatureNotSupportedException() throws Throwable {
    try (Connection connection = getConnection()) {
      PreparedStatement preparedStatement = connection.prepareStatement("select ?");
      expectFeatureNotSupportedException(
          () -> preparedStatement.setArray(1, new BaseJDBCTest.FakeArray()));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setAsciiStream(1, new BaseJDBCTest.FakeInputStream()));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setAsciiStream(1, new BaseJDBCTest.FakeInputStream(), 1));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setBinaryStream(1, new BaseJDBCTest.FakeInputStream()));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setBinaryStream(1, new BaseJDBCTest.FakeInputStream(), 1));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setCharacterStream(1, new BaseJDBCTest.FakeReader()));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setCharacterStream(1, new BaseJDBCTest.FakeReader(), 1));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setRef(1, new BaseJDBCTest.FakeRef()));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setBlob(1, new BaseJDBCTest.FakeBlob()));

      URL fakeURL = new URL("http://localhost:8888/");
      expectFeatureNotSupportedException(() -> preparedStatement.setURL(1, fakeURL));

      expectFeatureNotSupportedException(
          () -> preparedStatement.setRowId(1, new BaseJDBCTest.FakeRowId()));
      expectFeatureNotSupportedException(() -> preparedStatement.setNString(1, "test"));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setNCharacterStream(1, new BaseJDBCTest.FakeReader()));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setNCharacterStream(1, new BaseJDBCTest.FakeReader(), 1));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setNClob(1, new BaseJDBCTest.FakeNClob()));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setNClob(1, new BaseJDBCTest.FakeReader(), 1));

      expectFeatureNotSupportedException(
          () -> preparedStatement.setClob(1, new BaseJDBCTest.FakeReader()));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setClob(1, new BaseJDBCTest.FakeReader(), 1));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setBlob(1, new BaseJDBCTest.FakeInputStream()));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setBlob(1, new BaseJDBCTest.FakeInputStream(), 1));
      expectFeatureNotSupportedException(
          () -> preparedStatement.setSQLXML(1, new BaseJDBCTest.FakeSQLXML()));

      expectFeatureNotSupportedException(
          () -> preparedStatement.execute("insert into a values(1)", 1));
      expectFeatureNotSupportedException(
          () -> preparedStatement.execute("insert into a values(1)", new int[] {}));
      expectFeatureNotSupportedException(
          () -> preparedStatement.execute("insert into a values(1)", new String[] {}));
      expectFeatureNotSupportedException(
          () -> preparedStatement.executeUpdate("insert into a values(1)", 1));
      expectFeatureNotSupportedException(
          () -> preparedStatement.executeUpdate("insert into a values(1)", new int[] {}));
      expectFeatureNotSupportedException(
          () -> preparedStatement.executeUpdate("insert into a values(1)", new String[] {}));
      expectFeatureNotSupportedException(
          () -> preparedStatement.executeLargeUpdate("insert into a values(1)", 1));
      expectFeatureNotSupportedException(
          () -> preparedStatement.executeLargeUpdate("insert into a values(1)", new int[] {}));
      expectFeatureNotSupportedException(
          () -> preparedStatement.executeLargeUpdate("insert into a values(1)", new String[] {}));
    }
  }
}
