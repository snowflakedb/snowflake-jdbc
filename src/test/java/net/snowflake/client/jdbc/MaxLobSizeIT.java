package net.snowflake.client.jdbc;

import org.apache.commons.text.RandomStringGenerator;
import org.junit.Test;
import static org.junit.Assert.*;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class MaxLobSizeIT extends BaseJDBCTest {

    protected static String jsonQueryResultFormat = "json";

    //TODO: increase max size to 128 * 1024 * 1024
    public static int maxLobSize = 16 * 1024 * 1024;
    public static int largeLobSize = maxLobSize/2;
    public static int mediumLobSize = largeLobSize/2;
    public static int originLobSize = mediumLobSize/2;
    public static int smallLobSize = 16;

    public static String enableMaxLobSize = "alter session set FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY = 'ENABLED'";
    public static String insertQuery = "insert into my_lob_test (c1, c2, c3) values ('";
    public static String preparedInsertQuery = "insert into my_lob_test (c1, c2, c3) values (?, ?, ?)";
    public static String selectQuery = "select * from my_lob_test";

    public static String generateRandomString(int stringSize) {
        RandomStringGenerator randomStringGenerator = new RandomStringGenerator.Builder().withinRange('a','z').build();
        return randomStringGenerator.generate(stringSize);
    }

    public static Connection getConnection() throws SQLException {
        Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement();
//        stmt.execute(enableMaxLobSize);
        stmt.close();
        return con;
    }

    public static void setJSONResultFormat(Connection con) throws SQLException {
        Statement stmt = con.createStatement();
        stmt.execute("alter session set jdbc_query_result_format = '" + jsonQueryResultFormat + "'");
        stmt.close();
    }

    public void createTable(int lobSize, Connection con) throws SQLException {
        String createTableQuery = "create or replace table my_lob_test (c1 varchar, c2 varchar("
                + lobSize
                + "), c3 int)";

        Statement stmt = con.createStatement();
        stmt.execute(createTableQuery);
        stmt.close();
    }

    public void insertQuery(String varCharValue, int intValue, Connection con) throws SQLException {
        Statement stmt = con.createStatement();
        stmt.executeUpdate(insertQuery + varCharValue + "', '" + varCharValue + "', " + intValue + ")");
        stmt.close();
    }

    public void preparedInsertQuery(String varCharValue, int intValue, Connection con) throws SQLException {
        PreparedStatement pstmt = con.prepareStatement(preparedInsertQuery);

        pstmt.setString(1, varCharValue);
        pstmt.setString(2, varCharValue);
        pstmt.setInt(3, intValue);

        pstmt.execute();
        pstmt.close();
    }
    @Test
    public void testSmallLobSize() throws SQLException {
        Connection con = getConnection();
        createTable(smallLobSize, con);

        String smallVarCharValue = generateRandomString(smallLobSize);
        int intValue = new Random().nextInt();
        insertQuery(smallVarCharValue, intValue, con);

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(selectQuery);
        rs.next();
        assertEquals(smallVarCharValue, rs.getString(1));
        assertEquals(smallVarCharValue, rs.getString(2));
        assertEquals(intValue, rs.getInt(3));
        rs.close();

        // Execute select query again with JSON result format
        setJSONResultFormat(con);
        ResultSet rsJSON = stmt.executeQuery(selectQuery);
        rsJSON.next();
        assertEquals(smallVarCharValue, rsJSON.getString(1));
        assertEquals(smallVarCharValue, rsJSON.getString(2));
        assertEquals(intValue, rsJSON.getInt(3));
        rsJSON.close();

        stmt.close();
        con.close();
    }
    @Test
    public void testSmallLobSizePreparedInsert() throws SQLException {
        Connection con = getConnection();
        createTable(smallLobSize, con);

        String smallVarCharValue = generateRandomString(smallLobSize);
        int intValue = new Random().nextInt();
        preparedInsertQuery(smallVarCharValue, intValue, con);

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(selectQuery);
        rs.next();
        assertEquals(smallVarCharValue, rs.getString(1));
        assertEquals(smallVarCharValue, rs.getString(2));
        assertEquals(intValue, rs.getInt(3));
        rs.close();

        // Execute select query again with JSON result format
        setJSONResultFormat(con);
        ResultSet rsJSON = stmt.executeQuery(selectQuery);
        rsJSON.next();
        assertEquals(smallVarCharValue, rsJSON.getString(1));
        assertEquals(smallVarCharValue, rsJSON.getString(2));
        assertEquals(intValue, rsJSON.getInt(3));
        rsJSON.close();

        stmt.close();
        con.close();
    }

    @Test
    public void testOriginLobSize() throws SQLException {
        Connection con = getConnection();
        createTable(originLobSize, con);

        String originVarCharValue = generateRandomString(originLobSize);
        int intValue = new Random().nextInt();
        insertQuery(originVarCharValue, intValue, con);

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(selectQuery);
        rs.next();
        assertEquals(originVarCharValue, rs.getString(1));
        assertEquals(originVarCharValue, rs.getString(2));
        assertEquals(intValue, rs.getInt(3));
        rs.close();

        // Execute select query again with JSON result format
        setJSONResultFormat(con);
        ResultSet rsJSON = stmt.executeQuery(selectQuery);
        rsJSON.next();
        assertEquals(originVarCharValue, rsJSON.getString(1));
        assertEquals(originVarCharValue, rsJSON.getString(2));
        assertEquals(intValue, rsJSON.getInt(3));
        rsJSON.close();

        stmt.close();
        con.close();
    }
    @Test
    public void testOriginLobSizePreparedInsert() throws SQLException {
        Connection con = getConnection();
        createTable(originLobSize, con);

        String originVarCharValue = generateRandomString(originLobSize);
        int intValue = new Random().nextInt();
        preparedInsertQuery(originVarCharValue, intValue, con);

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(selectQuery);
        rs.next();
        assertEquals(originVarCharValue, rs.getString(1));
        assertEquals(originVarCharValue, rs.getString(2));
        assertEquals(intValue, rs.getInt(3));
        rs.close();

        // Execute select query again with JSON result format
        setJSONResultFormat(con);
        ResultSet rsJSON = stmt.executeQuery(selectQuery);
        rsJSON.next();
        assertEquals(originVarCharValue, rsJSON.getString(1));
        assertEquals(originVarCharValue, rsJSON.getString(2));
        assertEquals(intValue, rsJSON.getInt(3));
        rsJSON.close();

        stmt.close();
        con.close();
    }

    @Test
    public void testMediumLobSize() throws SQLException {
        Connection con = getConnection();
        createTable(mediumLobSize, con);

        String mediumVarCharValue = generateRandomString(mediumLobSize);
        int intValue = new Random().nextInt();
        insertQuery(mediumVarCharValue, intValue, con);

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(selectQuery);
        rs.next();
        assertEquals(mediumVarCharValue, rs.getString(1));
        assertEquals(mediumVarCharValue, rs.getString(2));
        assertEquals(intValue, rs.getInt(3));
        rs.close();

        // Execute select query again with JSON result format
        setJSONResultFormat(con);
        ResultSet rsJSON = stmt.executeQuery(selectQuery);
        rsJSON.next();
        assertEquals(mediumVarCharValue, rsJSON.getString(1));
        assertEquals(mediumVarCharValue, rsJSON.getString(2));
        assertEquals(intValue, rsJSON.getInt(3));
        rsJSON.close();

        stmt.close();
        con.close();
    }
    @Test
    public void testMediumLobSizePreparedInsert() throws SQLException {
        Connection con = getConnection();
        createTable(mediumLobSize, con);

        String mediumVarCharValue = generateRandomString(mediumLobSize);
        int intValue = new Random().nextInt();
        preparedInsertQuery(mediumVarCharValue, intValue, con);

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(selectQuery);
        rs.next();
        assertEquals(mediumVarCharValue, rs.getString(1));
        assertEquals(mediumVarCharValue, rs.getString(2));
        assertEquals(intValue, rs.getInt(3));
        rs.close();

        // Execute select query again with JSON result format
        setJSONResultFormat(con);
        ResultSet rsJSON = stmt.executeQuery(selectQuery);
        rsJSON.next();
        assertEquals(mediumVarCharValue, rsJSON.getString(1));
        assertEquals(mediumVarCharValue, rsJSON.getString(2));
        assertEquals(intValue, rsJSON.getInt(3));
        rsJSON.close();

        stmt.close();
        con.close();
    }

    @Test
    public void testLargeLobSize() throws SQLException {
        Connection con = getConnection();
        createTable(largeLobSize, con);

        String largeVarCharValue = generateRandomString(largeLobSize);
        int intValue = new Random().nextInt();
        insertQuery(largeVarCharValue, intValue, con);

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(selectQuery);
        rs.next();
        assertEquals(largeVarCharValue, rs.getString(1));
        assertEquals(largeVarCharValue, rs.getString(2));
        assertEquals(intValue, rs.getInt(3));
        rs.close();

        // Execute select query again with JSON result format
        setJSONResultFormat(con);
        ResultSet rsJSON = stmt.executeQuery(selectQuery);
        rsJSON.next();
        assertEquals(largeVarCharValue, rsJSON.getString(1));
        assertEquals(largeVarCharValue, rsJSON.getString(2));
        assertEquals(intValue, rsJSON.getInt(3));
        rsJSON.close();

        stmt.close();
        con.close();
    }
    @Test
    public void testLargeLobSizePreparedInsert() throws SQLException {
        Connection con = getConnection();
        createTable(largeLobSize, con);

        String largeVarCharValue = generateRandomString(largeLobSize);
        int intValue = new Random().nextInt();
        preparedInsertQuery(largeVarCharValue, intValue, con);

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(selectQuery);
        rs.next();
        assertEquals(largeVarCharValue, rs.getString(1));
        assertEquals(largeVarCharValue, rs.getString(2));
        assertEquals(intValue, rs.getInt(3));
        rs.close();

        // Execute select query again with JSON result format
        setJSONResultFormat(con);
        ResultSet rsJSON = stmt.executeQuery(selectQuery);
        rsJSON.next();
        assertEquals(largeVarCharValue, rsJSON.getString(1));
        assertEquals(largeVarCharValue, rsJSON.getString(2));
        assertEquals(intValue, rsJSON.getInt(3));
        rsJSON.close();

        stmt.close();
        con.close();
    }

    @Test
    public void testMaxLobSize() throws SQLException {
        Connection con = getConnection();
        createTable(maxLobSize, con);

        String maxVarCharValue = generateRandomString(maxLobSize);
        int intValue = new Random().nextInt();
        insertQuery(maxVarCharValue, intValue, con);

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(selectQuery);
        rs.next();
        assertEquals(maxVarCharValue, rs.getString(1));
        assertEquals(maxVarCharValue, rs.getString(2));
        assertEquals(intValue, rs.getInt(3));
        rs.close();

        // Execute select query again with JSON result format
        setJSONResultFormat(con);
        ResultSet rsJSON = stmt.executeQuery(selectQuery);
        rsJSON.next();
        assertEquals(maxVarCharValue, rsJSON.getString(1));
        assertEquals(maxVarCharValue, rsJSON.getString(2));
        assertEquals(intValue, rsJSON.getInt(3));
        rsJSON.close();

        stmt.close();
        con.close();
    }
    @Test
    public void testMaxLobSizePreparedInsert() throws SQLException {
        Connection con = getConnection();
        createTable(maxLobSize, con);

        String maxVarCharValue = generateRandomString(maxLobSize);
        int intValue = new Random().nextInt();
        preparedInsertQuery(maxVarCharValue, intValue, con);

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(selectQuery);
        rs.next();
        assertEquals(maxVarCharValue, rs.getString(1));
        assertEquals(maxVarCharValue, rs.getString(2));
        assertEquals(intValue, rs.getInt(3));
        rs.close();

        // Execute select query again with JSON result format
        setJSONResultFormat(con);
        ResultSet rsJSON = stmt.executeQuery(selectQuery);
        rsJSON.next();
        assertEquals(maxVarCharValue, rsJSON.getString(1));
        assertEquals(maxVarCharValue, rsJSON.getString(2));
        assertEquals(intValue, rsJSON.getInt(3));
        rsJSON.close();

        stmt.close();
        con.close();
    }

}
