package net.snowflake.client.loader;

import java.sql.Connection;
import java.sql.SQLException;
import net.snowflake.client.AbstractDriverIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class LoaderBase {
  static final String TARGET_TABLE_NAME = "LOADER_test_TABLE";

  static Connection testConnection;
  static Connection putConnection;
  static String SCHEMA_NAME;

  @BeforeAll
  public static void setUpClass() throws Throwable {
    testConnection = AbstractDriverIT.getConnection();
    putConnection = AbstractDriverIT.getConnection();

    SCHEMA_NAME = testConnection.getSchema();
    testConnection
        .createStatement()
        .execute(
            String.format(
                "CREATE OR REPLACE TABLE \"%s\" ("
                    + "ID int, "
                    + "C1 varchar(255), "
                    + "C2 varchar(255) DEFAULT 'X', "
                    + "C3 double, "
                    + "C4 timestamp, "
                    + "C5 variant)",
                LoaderIT.TARGET_TABLE_NAME));

    testConnection
        .createStatement()
        .execute("alter session set JDBC_QUERY_RESULT_FORMAT='ARROW', QUERY_RESULT_FORMAT='ARROW'");
  }

  @AfterAll
  public static void tearDownClass() throws SQLException {
    testConnection
        .createStatement()
        .execute(String.format("DROP TABLE IF EXISTS \"%s\"", LoaderIT.TARGET_TABLE_NAME));

    testConnection.close();
    putConnection.close();
  }
}
