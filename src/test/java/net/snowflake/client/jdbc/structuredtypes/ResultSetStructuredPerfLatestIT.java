/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc.structuredtypes;

import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.ThrowingConsumer;
import net.snowflake.client.category.TestCategoryResultSet;
import net.snowflake.client.core.structs.SnowflakeObjectTypeFactories;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.SnowflakePreparedStatementV1;
import net.snowflake.client.jdbc.structuredtypes.sqldata.AllTypesClass;
import net.snowflake.client.jdbc.structuredtypes.sqldata.SimpleClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Category(TestCategoryResultSet.class)
public class ResultSetStructuredPerfLatestIT extends BaseJDBCTest {

  @Parameterized.Parameters(name = "format={0}")
  public static Object[][] data() {
    return new Object[][] {
      {ResultSetFormatType.JSON},
      {ResultSetFormatType.ARROW_WITH_JSON_STRUCTURED_TYPES},
      {ResultSetFormatType.NATIVE_ARROW}
    };
  }

  private final ResultSetFormatType queryResultFormat;

  public ResultSetStructuredPerfLatestIT(ResultSetFormatType queryResultFormat) {
    this.queryResultFormat = queryResultFormat;
  }

  public Connection init() throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE = true");
      stmt.execute("alter session set IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE = true");
      stmt.execute("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'");
      stmt.execute(
          "alter session set jdbc_query_result_format = '"
              + queryResultFormat.sessionParameterTypeValue
              + "'");
      if (queryResultFormat == ResultSetFormatType.NATIVE_ARROW) {
        stmt.execute("alter session set ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT = true");
        stmt.execute("alter session set FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT = true");
      }
    }
    return conn;
  }

  @Before
  public void clean() throws Exception {
    SnowflakeObjectTypeFactories.unregister(SimpleClass.class);
    SnowflakeObjectTypeFactories.unregister(AllTypesClass.class);
  }

    @Test
    @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
    public void testWriteReadManyObject() throws SQLException {
        SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
        int count = 10000;
        try (Connection connection = init()) {
            Statement statement = connection.createStatement();
            statement.execute(
                    "CREATE OR REPLACE TABLE test_table (ob OBJECT(string varchar, intValue NUMBER))");

            statement.executeUpdate("insert into test_table select {'string': randstr(1000,random()), 'intValue': 2}::OBJECT(string VARCHAR, intValue INTEGER) from table(generator(rowcount=>"+count+"))");

            PreparedStatement stmt3 =
                            connection.prepareStatement("SELECT ob FROM test_table");
            ResultSet resultSet = stmt3.executeQuery();
            System.out.println("Start" + LocalDateTime.now());
            List<SimpleClass> result = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                resultSet.next();
                result.add(resultSet.getObject(1, SimpleClass.class));
            }
            System.out.println("Finish" + LocalDateTime.now());
        }
    }

    @Test
    @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
    public void testWriteReadManyStrings() throws SQLException {
        SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
        int count = 100000;
        try (Connection connection = init()) {
            Statement statement = connection.createStatement();
            statement.execute(
                    "CREATE OR REPLACE TABLE test_table (ob VARCHAR)");


            statement.execute("insert into test_table select randstr(1000,random()) from table(generator(rowcount=>"+count+"))");
            System.out.println("Start" + LocalDateTime.now());

            PreparedStatement stmt3 =
                            connection.prepareStatement("SELECT ob FROM test_table");

            ResultSet resultSet = stmt3.executeQuery();
            System.out.println("Start2" + LocalDateTime.now());
            List<String> result = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                resultSet.next();
                result.add(resultSet.getString(1));
            }
            System.out.println("Finish" + LocalDateTime.now());
        }
    }

  private void withFirstRow(String sqlText, ThrowingConsumer<ResultSet, SQLException> consumer)
      throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sqlText); ) {
      assertTrue(rs.next());
      consumer.accept(rs);
    }
  }

  enum ResultSetFormatType {
    JSON("JSON"),
    ARROW_WITH_JSON_STRUCTURED_TYPES("ARROW"),
    NATIVE_ARROW("ARROW");

    public final String sessionParameterTypeValue;

    ResultSetFormatType(String sessionParameterTypeValue) {
      this.sessionParameterTypeValue = sessionParameterTypeValue;
    }
  }
}
