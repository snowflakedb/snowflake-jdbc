package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class CallableStatementITBase extends BaseJDBCTest {
  public static Connection getConnection() throws SQLException {
    return BaseJDBCTest.getConnection();
  }

  public static Connection getConnection(String queryResultFormat) throws SQLException {
    Connection conn = BaseJDBCTest.getConnection();
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    }
    return conn;
  }

  private final String createStoredProcedure =
      "create or replace procedure square_it(num FLOAT) returns float not "
          + "null language javascript as $$ return NUM * NUM; $$";
  private final String createSecondStoredProcedure =
      "create or replace procedure add_nums(x DOUBLE, y DOUBLE) "
          + "returns double not null language javascript as $$ return X + Y; $$";
  private final String deleteStoredProcedure = "drop procedure if exists square_it(FLOAT)";
  private final String deleteSecondStoredProcedure = "drop procedure if exists add_nums(INT, INT)";

  @BeforeEach
  public void setUp() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      statement.execute(createStoredProcedure);
      statement.execute(createSecondStoredProcedure);
    }
  }

  @AfterEach
  public void tearDown() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      statement.execute(deleteStoredProcedure);
      statement.execute(deleteSecondStoredProcedure);
    }
  }
}
