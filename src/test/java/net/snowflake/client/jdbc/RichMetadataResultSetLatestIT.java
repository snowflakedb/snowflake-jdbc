package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryResultSet;
import net.snowflake.client.core.QueryStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Category(TestCategoryResultSet.class)
public class RichMetadataResultSetLatestIT extends BaseJDBCTest {
    private final String queryResultFormat = "json";

    public Connection init() throws SQLException {
        Connection conn = BaseJDBCTest.getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
        Statement stmt = conn.createStatement();
        stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
        stmt.close();
        return conn;
    }

    @Before
    public void setUp() throws SQLException {
        Connection con = init();
        con.createStatement().execute(
                "create or replace table test_rich_rs (c1 number, c2 string) as " +
                        "(select seq4() as seq, concat('str', to_varchar(seq)) as str1 " +
                        "from table(generator(rowcount => 2000000)))"
        );
    }

    @Test
    public void testAsyncRichResultSetMetadata() throws Exception {
        Connection connection = init();
        Statement statement = connection.createStatement();

        // Perform async query
        ResultSet rs = statement
                .unwrap(SnowflakeStatement.class)
                .executeAsyncQuery("select sum(c1), avg(c1) from test_rich_rs group by (c1 % 200000)");

        // Wait until it's finished (Thread.sleep to keep POC simple)
        Thread.sleep(5000);

        QueryStatusV2 statusV2 = rs.unwrap(SnowflakeResultSet.class).getStatusV2();
        if (statusV2.getStatus() == QueryStatus.SUCCESS) {
            String queryId = rs.unwrap(SnowflakeResultSet.class).getQueryID();

            // Query for /query/{queryId}/results?resultLoadingMode=BOTH_MAIN_AND_RICH_RESULT to get actual data with rich metadata
            ResultSet resultSetWithRichMetadata =
                    connection.unwrap(SnowflakeConnection.class).createResultSet(queryId);
            while (resultSetWithRichMetadata.next()) {
                long sumResult = resultSetWithRichMetadata.getLong(1);
                float avgResult = resultSetWithRichMetadata.getFloat(2);
                System.out.println("SUM: " + sumResult + ", AVG: " + avgResult);
            }
        }
        statement.close();
        connection.close();
    }

    @Test
    public void testRichResultSetMetadata() throws Exception {
        Connection connection = init();
        Statement statement = connection.createStatement();

        // Perform query with rich result set
        RichResultSet resultSetWithRichMetadata = statement
                .unwrap(SnowflakeStatement.class)
                .executeRichResultsQuery("select sum(c1), avg(c1) from test_rich_rs group by (c1 % 200000)");

        while (resultSetWithRichMetadata.next()) {
            long sumResult = resultSetWithRichMetadata.getLong(1);
            float avgResult = resultSetWithRichMetadata.getFloat(2);
            System.out.println("SUM: " + sumResult + ", AVG: " + avgResult);
        }

        statement.close();
        connection.close();
    }

    @After
    public void tearDown() throws SQLException {
        Connection con = init();
        con.createStatement().execute("drop table if exists test_rich_rs");
        con.close();
    }
}