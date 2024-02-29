package net.snowflake.client.jdbc;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import net.snowflake.client.core.ObjectMapperFactory;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MaxLobSizeIT extends BaseJDBCTest {

    protected static String jsonQueryResultFormat = "json";

    // TODO: increase max size to 128 * 1024 * 1024
    private static int maxLobSize = 16 * 1024 * 1024;
    private static int largeLobSize = maxLobSize / 2;
    private static int mediumLobSize = largeLobSize / 2;
    private static int originLobSize = mediumLobSize / 2;
    private static int smallLobSize = 16;

    @BeforeClass
    public static void setUp() {
        // TODO: May need to increase MAX_JSON_STRING_LENGTH_JVM once we increase the max LOB size.
        System.setProperty(
                ObjectMapperFactory.MAX_JSON_STRING_LENGTH_JVM, Integer.toString(45_000_000));
    }

    @Parameterized.Parameters
    public static Collection<Integer> data() {
        int[] lobSizes =
                new int[] {smallLobSize, originLobSize, mediumLobSize, largeLobSize, maxLobSize};
        List<Integer> ret = new ArrayList<>();
        for (int i = 0; i < lobSizes.length; i++) {
            ret.add(lobSizes[i]);
        }
        return ret;
    }

    private final int lobSize;

    public MaxLobSizeIT(int lobSize) {
        this.lobSize = lobSize;
    }

    private static String enableMaxLobSize =
            "alter session set FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY = 'ENABLED'";
    private static String insertQuery = "insert into my_lob_test (c1, c2, c3) values ('";
    private static String preparedInsertQuery =
            "insert into my_lob_test (c1, c2, c3) values (?, ?, ?)";
    private static String selectQuery = "select * from my_lob_test";

    private static String tableName = "my_lob_test";

    private static String generateRandomString(int stringSize) {
        RandomStringGenerator randomStringGenerator =
                new RandomStringGenerator.Builder().withinRange('a', 'z').build();
        return randomStringGenerator.generate(stringSize);
    }

    private static void setJSONResultFormat(Connection con) throws SQLException {
        try (Statement stmt = con.createStatement()) {
            stmt.execute("alter session set jdbc_query_result_format = '" + jsonQueryResultFormat + "'");
        }
    }

    private void createTable(int lobSize, Connection con) throws SQLException {
        try (Statement stmt = con.createStatement()) {
            String createTableQuery =
                    "create or replace table my_lob_test (c1 varchar, c2 varchar(" + lobSize + "), c3 int)";
            stmt.execute(createTableQuery);
        }
    }

    private void insertQuery(String varCharValue, int intValue, Connection con) throws SQLException {
        try (Statement stmt = con.createStatement()) {
            stmt.executeUpdate(
                    insertQuery + varCharValue + "', '" + varCharValue + "', " + intValue + ")");
        }
    }

    private void preparedInsertQuery(String varCharValue, int intValue, Connection con)
            throws SQLException {
        try (PreparedStatement pstmt = con.prepareStatement(preparedInsertQuery)) {
            pstmt.setString(1, varCharValue);
            pstmt.setString(2, varCharValue);
            pstmt.setInt(3, intValue);

            pstmt.execute();
        }
    }

    @After
    public void tearDown() throws SQLException {
        try (Connection con = BaseJDBCTest.getConnection();
             Statement stmt = con.createStatement()) {
            stmt.execute("Drop table if exists " + tableName);
        }
    }

    @Test
    public void testStandardInsertAndSelectWithMaxLobSizeEnabled() throws SQLException {
        try (Connection con = BaseJDBCTest.getConnection();
             Statement stmt = con.createStatement()) {
            // TODO: Uncomment when ready to test with max lob size enabled.
            // stmt.execute(enableMaxLobSize);
            createTable(lobSize, con);

            String varCharValue = generateRandomString(lobSize);
            int intValue = new Random().nextInt();
            insertQuery(varCharValue, intValue, con);

            try (ResultSet rs = stmt.executeQuery(selectQuery)) {
                rs.next();
                assertEquals(varCharValue, rs.getString(1));
                assertEquals(varCharValue, rs.getString(2));
                assertEquals(intValue, rs.getInt(3));
            }

            // Execute select query again with JSON result format
            setJSONResultFormat(con);
            try (ResultSet rsJSON = stmt.executeQuery(selectQuery)) {
                rsJSON.next();
                assertEquals(varCharValue, rsJSON.getString(1));
                assertEquals(varCharValue, rsJSON.getString(2));
                assertEquals(intValue, rsJSON.getInt(3));
            }
        }
    }

    @Test
    public void testPreparedInsertWithMaxLobSizeEnabled() throws SQLException {
        try (Connection con = BaseJDBCTest.getConnection();
             Statement stmt = con.createStatement()) {
            // TODO: Uncomment when ready to test with max lob size enabled.
            // stmt.execute(enableMaxLobSize);

            createTable(lobSize, con);

            String maxVarCharValue = generateRandomString(lobSize);
            int intValue = new Random().nextInt();
            preparedInsertQuery(maxVarCharValue, intValue, con);

            try (ResultSet rs = stmt.executeQuery(selectQuery)) {
                rs.next();
                assertEquals(maxVarCharValue, rs.getString(1));
                assertEquals(maxVarCharValue, rs.getString(2));
                assertEquals(intValue, rs.getInt(3));
            }

            // Execute select query again with JSON result format
            setJSONResultFormat(con);

            try (ResultSet rsJSON = stmt.executeQuery(selectQuery)) {
                rsJSON.next();
                assertEquals(maxVarCharValue, rsJSON.getString(1));
                assertEquals(maxVarCharValue, rsJSON.getString(2));
                assertEquals(intValue, rsJSON.getInt(3));
            }
        }
    }

    @Test
    public void testPutAndGet() throws IOException, SQLException {
        File tempFile = File.createTempFile("LobSizeTest", ".csv");
        // Delete file when JVM shuts down
        tempFile.deleteOnExit();

        String filePath = tempFile.getPath();
        String filePathEscaped = filePath.replace("\\", "\\\\");
        String fileName = tempFile.getName();

        String varCharValue = generateRandomString(lobSize);
        int intValue = new Random().nextInt();
        String fileInput = varCharValue + "," + varCharValue + "," + intValue;

        // Print data to new temporary file
        try (PrintWriter out = new PrintWriter(filePath)) {
            out.println(fileInput);
        }

        try (Connection con = BaseJDBCTest.getConnection();
             Statement stmt = con.createStatement()) {
            // TODO: Uncomment when ready to test with max lob size enabled.
            // stmt.execute(enableMaxLobSize);

            createTable(lobSize, con);

            // Test PUT
            String sqlPut = "PUT 'file://" + filePathEscaped + "' @%" + tableName;

            stmt.execute(sqlPut);

            try (ResultSet rsPut = stmt.getResultSet()) {
                assertTrue(rsPut.next());
                assertEquals(fileName, rsPut.getString(1));
                assertEquals(fileName + ".gz", rsPut.getString(2));
                assertEquals("GZIP", rsPut.getString(6));
                assertEquals("UPLOADED", rsPut.getString(7));
            }

            try (ResultSet rsFiles = stmt.executeQuery("ls @%" + tableName)) {
                // ResultSet should return a row with the zipped file name
                assertTrue(rsFiles.next());
                assertEquals(fileName + ".gz", rsFiles.getString(1));
            }

            String copyInto =
                    "copy into "
                            + tableName
                            + " from @%"
                            + tableName
                            + " file_format=(type=csv compression='gzip')";
            stmt.execute(copyInto);

            // Check that results are copied into table correctly
            try (ResultSet rsCopy = stmt.executeQuery("Select * from " + tableName)) {
                assertTrue(rsCopy.next());
                assertEquals(varCharValue, rsCopy.getString(1));
                assertEquals(varCharValue, rsCopy.getString(2));
                assertEquals(intValue, rsCopy.getInt(3));
            }

            // Test Get
            Path tempDir = Files.createTempDirectory("MaxLobTest");
            // Delete tempDir when JVM shuts down
            tempDir.toFile().deleteOnExit();
            String pathToTempDir = tempDir.toString().replace("\\", "\\\\");

            String getSql = "get @%" + tableName + " 'file://" + pathToTempDir + "'";
            stmt.execute(getSql);

            try (ResultSet rsGet = stmt.getResultSet()) {
                assertTrue(rsGet.next());
                assertEquals(fileName + ".gz", rsGet.getString(1));
                assertEquals("DOWNLOADED", rsGet.getString(3));
                assertEquals("DECRYPTED", rsGet.getString(4));
            }
        }
    }
}
