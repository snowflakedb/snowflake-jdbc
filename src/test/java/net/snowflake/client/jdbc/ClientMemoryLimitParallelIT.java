package net.snowflake.client.jdbc;

import static net.snowflake.client.AbstractDriverIT.getConnection;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** attempts to test the CLIENT_MEMORY_LIMIT working in multi-threading */
@Tag(TestTags.OTHERS)
public class ClientMemoryLimitParallelIT extends BaseJDBCWithSharedConnectionIT {
  private static Logger LOGGER =
      LoggerFactory.getLogger(ClientMemoryLimitParallelIT.class.getName());

  private static final int rowCount = 10000;
  private static final String createTestTableSQL =
      "create or replace table testtable_cml \n"
          + "(c1 number, c2 number, c3 number, c4 number,\n"
          + "c5 number, c6 number, c7 number, c8 number,\n"
          + "c9 number, c10 number, c11 number, c12 number,\n"
          + "c13 text, c14 text, c15 text, c16 text, c17 text, c18 text, c19 text, "
          + "c20 text, c21 text, c22 text,\n"
          + "c23 text, c24 text, c25 text, c26 text, c27 text, c28 text, c29 text, "
          + "c30 text, c31 text, c32 text,\n"
          + "c33 text, c34 text, c35 text, c36 text, c37 text, c38 text, c39 text, "
          + "c40 text, c41 text, c42 text,\n"
          + "c43 text, c44 text, c45 text, c46 text, c47 text, c48 text, c49 text, "
          + "c50 text, c51 text, c52 text,\n"
          + "c53 text, c54 text, c55 text, c56 text, c57 text, c58 text, c59 text, "
          + "c60 text)\n"
          + "as\n"
          + "select \n"
          + "seq1(), seq2()+10, seq4()+100, seq8()+1000, \n"
          + "seq1(), seq2()+10, seq4()+100, seq8()+1000, \n"
          + "seq1(), seq2()+10, seq4()+100, seq8()+1000, \n"
          + "random(),random(1),random(10),random(100),\n"
          + "random(),random(1),random(10),random(100),\n"
          + "random(),random(1),random(10),random(100),\n"
          + "random(),random(1),random(10),random(100),\n"
          + "random(),random(1),random(10),random(100),\n"
          + "random(),random(1),random(10),random(100),\n"
          + "random(),random(1),random(10),random(100),\n"
          + "random(),random(1),random(10),random(100),\n"
          + "random(),random(1),random(10),random(100),\n"
          + "random(),random(1),random(10),random(100),\n"
          + "random(),random(1),random(10),random(100),\n"
          + "random(),random(1),random(10),random(100)\n"
          + "from table(generator(rowcount => "
          + rowCount
          + "));";

  @BeforeEach
  public void setUp() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(createTestTableSQL);
    }
  }

  @AfterEach
  public void tearDown() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute("drop table if exists testtable_cml");
    }
  }

  /**
   * This test attempts to set small CLIENT_MEMORY_LIMIT and client chunk size to make sure no OOM
   * in multi-threading
   */
  @Test
  @Disabled("Long term high memory usage test")
  void testParallelQueries() throws Exception {
    Runnable testQuery =
        new Runnable() {
          public void run() {
            try {
              Properties paramProperties = new Properties();
              try (Connection connection = getConnection(paramProperties);
                  Statement statement = connection.createStatement()) {
                queryRows(statement, 100, 48);
              }
            } catch (SQLException e) {
              // do not expect exception in test
              assertEquals(null, e);
            }
          }
        };
    Thread t1 = new Thread(testQuery);
    Thread t2 = new Thread(testQuery);
    Thread t3 = new Thread(testQuery);
    Thread t4 = new Thread(testQuery);
    Thread t5 = new Thread(testQuery);

    t1.start();
    t2.start();
    t3.start();
    t4.start();
    t5.start();

    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
  }

  /**
   * This test attempts to set CLIENT_MEMORY_LIMIT smaller than the maximum client chunk size and
   * make sure there is no hanging
   */
  @Test
  void testQueryNotHanging() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      queryRows(statement, 100, 160);
    }
  }

  private static void queryRows(Statement stmt, int limit, int chunkSize) throws SQLException {
    stmt.execute("alter session set CLIENT_MEMORY_LIMIT=" + limit);
    stmt.execute("alter session set CLIENT_RESULT_CHUNK_SIZE=" + chunkSize);
    String query;
    query = "select * from testtable_cml";

    try (ResultSet resultSet = stmt.executeQuery(query)) {

      // fetch data
      int rowIdx = 0;
      while (resultSet.next()) {
        rowIdx++;
        if (rowIdx % 1000 == 0) {
          LOGGER.info(Thread.currentThread().getName() + ": processedRows: " + rowIdx);
        }
      }
      assertEquals(rowIdx, rowCount);
    }
  }
}
