package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SnowflakeDatabaseMetaDataPatternSearchTest {
  private static final String DATABASE = "TEST_DATABASE";
  private static final String SCHEMA = "TEST_SCHEMA";
  private static final String TABLE1 = "PATTERN_SEARCH_TABLE1";
  private static final String TABLE2 = "PATTERN_SEARCH_TABLE2";

  private static final String SCHEMA_PATTERN_PERCENT =
      SCHEMA.substring(0, SCHEMA.length() - 1).concat("%");
  private static final String SCHEMA_PATTERN_UNDERSCORE =
      SCHEMA.substring(0, SCHEMA.length() - 1).concat("_");
  private static final String TABLE_PATTERN_PERCENT = "PATTERN_SEARCH_TABLE%";
  private static final String TABLE_PATTERN_UNDERSCORE = "PATTERN_SEARCH_TABLE_";

  private static void setupPrimaryKeysMock(Statement mockStatement) throws SQLException {
    when(mockStatement.executeQuery(anyString()))
        .thenAnswer(
            invocation -> {
              ResultSet rs = mock(ResultSet.class);
              when(rs.next()).thenReturn(true, false);
              when(rs.getString(2)).thenReturn(DATABASE);
              when(rs.getString(3)).thenReturn(SCHEMA);
              when(rs.getString(4)).thenReturn(TABLE1);
              when(rs.getString(5)).thenReturn("C1");
              when(rs.getInt(6)).thenReturn(1);
              when(rs.getString(7)).thenReturn("PK_TEST");
              when(rs.isClosed()).thenReturn(false);
              return rs;
            });
  }

  private static void setupForeignKeysMock(Statement mockStatement) throws SQLException {
    when(mockStatement.executeQuery(anyString()))
        .thenAnswer(
            invocation -> {
              ResultSet rs = mock(ResultSet.class);
              when(rs.next()).thenReturn(true, false);
              when(rs.getString(2)).thenReturn(DATABASE);
              when(rs.getString(3)).thenReturn(SCHEMA);
              when(rs.getString(4)).thenReturn(TABLE1);
              when(rs.getString(5)).thenReturn("C1");
              when(rs.getString(6)).thenReturn(DATABASE);
              when(rs.getString(7)).thenReturn(SCHEMA);
              when(rs.getString(8)).thenReturn(TABLE2);
              when(rs.getString(9)).thenReturn("C3");
              when(rs.getInt(10)).thenReturn(1);
              when(rs.getString(11)).thenReturn("FK_TEST");
              when(rs.getString(12)).thenReturn("PK_TEST");
              when(rs.getString(13)).thenReturn("NONE");
              when(rs.getString(14)).thenReturn("NONE");
              when(rs.getString(15)).thenReturn("NOT_DEFERRABLE");
              when(rs.isClosed()).thenReturn(false);
              return rs;
            });
  }

  private static int getSizeOfResultSet(ResultSet rs) throws SQLException {
    int count = 0;
    while (rs.next()) {
      count++;
    }
    return count;
  }

  @Nested
  class WhenPatternSearchDisabled {

    @Mock private SnowflakeConnectionV1 mockConnection;
    @Mock private SFBaseSession mockSession;
    @Mock private Statement mockStatement;
    @Mock private Telemetry mockTelemetry;

    private SnowflakeDatabaseMetaData databaseMetaData;

    @BeforeEach
    public void setUp() throws SQLException {
      MockitoAnnotations.openMocks(this);

      when(mockSession.getEnablePatternSearch()).thenReturn(false);
      when(mockSession.getMetadataRequestUseConnectionCtx()).thenReturn(false);
      when(mockSession.getMetadataRequestUseSessionDatabase()).thenReturn(false);
      when(mockSession.isStringQuoted()).thenReturn(false);
      when(mockSession.getEnableExactSchemaSearch()).thenReturn(false);
      when(mockSession.getEnableWildcardsInShowMetadataCommands()).thenReturn(false);
      when(mockSession.getTelemetryClient()).thenReturn(mockTelemetry);

      when(mockConnection.unwrap(SnowflakeConnectionV1.class)).thenReturn(mockConnection);
      when(mockConnection.getSFBaseSession()).thenReturn(mockSession);
      when(mockConnection.createStatement()).thenReturn(mockStatement);
      when(mockConnection.getCatalog()).thenReturn(DATABASE);
      when(mockConnection.getSchema()).thenReturn(SCHEMA);

      when(mockStatement.getConnection()).thenReturn(mockConnection);

      databaseMetaData = new SnowflakeDatabaseMetaData(mockConnection);
    }

    @Test
    public void testNoPatternSearchAllowedForPrimaryKeys() throws SQLException {
      setupPrimaryKeysMock(mockStatement);

      assertEquals(
          1, getSizeOfResultSet(databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA, TABLE1)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA_PATTERN_PERCENT, TABLE1)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA_PATTERN_UNDERSCORE, TABLE1)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA_PATTERN_PERCENT, null)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA_PATTERN_UNDERSCORE, null)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA, TABLE_PATTERN_PERCENT)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA, TABLE_PATTERN_UNDERSCORE)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, null, TABLE_PATTERN_PERCENT)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, null, TABLE_PATTERN_UNDERSCORE)));
    }

    @Test
    public void testNoPatternSearchAllowedForImportedKeys() throws SQLException {
      setupForeignKeysMock(mockStatement);

      assertEquals(
          1, getSizeOfResultSet(databaseMetaData.getImportedKeys(DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, SCHEMA_PATTERN_PERCENT, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, SCHEMA_PATTERN_PERCENT, null)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, SCHEMA_PATTERN_UNDERSCORE, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, SCHEMA_PATTERN_UNDERSCORE, null)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, null, TABLE_PATTERN_PERCENT)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, null, TABLE_PATTERN_UNDERSCORE)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, SCHEMA, TABLE_PATTERN_PERCENT)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, SCHEMA, TABLE_PATTERN_UNDERSCORE)));
    }

    @Test
    public void testNoPatternSearchAllowedForExportedKeys() throws SQLException {
      setupForeignKeysMock(mockStatement);

      assertEquals(
          1, getSizeOfResultSet(databaseMetaData.getExportedKeys(DATABASE, SCHEMA, TABLE1)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getExportedKeys(DATABASE, SCHEMA_PATTERN_PERCENT, TABLE1)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getExportedKeys(DATABASE, null, TABLE_PATTERN_PERCENT)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getExportedKeys(DATABASE, null, TABLE_PATTERN_UNDERSCORE)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getExportedKeys(DATABASE, SCHEMA, TABLE_PATTERN_PERCENT)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getExportedKeys(DATABASE, SCHEMA, TABLE_PATTERN_UNDERSCORE)));
    }

    @Test
    public void testNoPatternSearchAllowedForCrossReference() throws SQLException {
      setupForeignKeysMock(mockStatement);

      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA_PATTERN_PERCENT, TABLE1, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA_PATTERN_UNDERSCORE, TABLE1, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA_PATTERN_PERCENT, null, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA_PATTERN_UNDERSCORE, null, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA_PATTERN_PERCENT, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA_PATTERN_UNDERSCORE, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA_PATTERN_PERCENT, null)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA_PATTERN_UNDERSCORE, null)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, null, TABLE_PATTERN_PERCENT, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, null, TABLE_PATTERN_UNDERSCORE, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE_PATTERN_PERCENT, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE_PATTERN_UNDERSCORE, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, null, TABLE_PATTERN_PERCENT)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, null, TABLE_PATTERN_UNDERSCORE)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA, TABLE_PATTERN_PERCENT)));
      assertEquals(
          0,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA, TABLE_PATTERN_UNDERSCORE)));
    }
  }

  @Nested
  class WhenPatternSearchEnabled {

    @Mock private SnowflakeConnectionV1 mockConnection;
    @Mock private SFBaseSession mockSession;
    @Mock private Statement mockStatement;
    @Mock private Telemetry mockTelemetry;

    private SnowflakeDatabaseMetaData databaseMetaData;

    @BeforeEach
    public void setUp() throws SQLException {
      MockitoAnnotations.openMocks(this);

      when(mockSession.getEnablePatternSearch()).thenReturn(true);
      when(mockSession.getMetadataRequestUseConnectionCtx()).thenReturn(false);
      when(mockSession.getMetadataRequestUseSessionDatabase()).thenReturn(false);
      when(mockSession.isStringQuoted()).thenReturn(false);
      when(mockSession.getEnableExactSchemaSearch()).thenReturn(false);
      when(mockSession.getEnableWildcardsInShowMetadataCommands()).thenReturn(false);
      when(mockSession.getTelemetryClient()).thenReturn(mockTelemetry);

      when(mockConnection.unwrap(SnowflakeConnectionV1.class)).thenReturn(mockConnection);
      when(mockConnection.getSFBaseSession()).thenReturn(mockSession);
      when(mockConnection.createStatement()).thenReturn(mockStatement);
      when(mockConnection.getCatalog()).thenReturn(DATABASE);
      when(mockConnection.getSchema()).thenReturn(SCHEMA);

      when(mockStatement.getConnection()).thenReturn(mockConnection);

      databaseMetaData = new SnowflakeDatabaseMetaData(mockConnection);
    }

    @Test
    public void testPatternSearchEnabledForPrimaryKeys() throws SQLException {
      setupPrimaryKeysMock(mockStatement);

      assertEquals(
          1, getSizeOfResultSet(databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA, TABLE1)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA_PATTERN_PERCENT, TABLE1)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA_PATTERN_UNDERSCORE, TABLE1)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA_PATTERN_PERCENT, null)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA_PATTERN_UNDERSCORE, null)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA, TABLE_PATTERN_PERCENT)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, SCHEMA, TABLE_PATTERN_UNDERSCORE)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, null, TABLE_PATTERN_PERCENT)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getPrimaryKeys(DATABASE, null, TABLE_PATTERN_UNDERSCORE)));
    }

    @Test
    public void testPatternSearchEnabledForImportedKeys() throws SQLException {
      setupForeignKeysMock(mockStatement);

      assertEquals(
          1, getSizeOfResultSet(databaseMetaData.getImportedKeys(DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, SCHEMA_PATTERN_PERCENT, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, SCHEMA_PATTERN_PERCENT, null)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, SCHEMA_PATTERN_UNDERSCORE, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, SCHEMA_PATTERN_UNDERSCORE, null)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, null, TABLE_PATTERN_PERCENT)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, null, TABLE_PATTERN_UNDERSCORE)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, SCHEMA, TABLE_PATTERN_PERCENT)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getImportedKeys(DATABASE, SCHEMA, TABLE_PATTERN_UNDERSCORE)));
    }

    @Test
    public void testPatternSearchEnabledForExportedKeys() throws SQLException {
      setupForeignKeysMock(mockStatement);

      assertEquals(
          1, getSizeOfResultSet(databaseMetaData.getExportedKeys(DATABASE, SCHEMA, TABLE1)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getExportedKeys(DATABASE, SCHEMA_PATTERN_PERCENT, TABLE1)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getExportedKeys(DATABASE, null, TABLE_PATTERN_PERCENT)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getExportedKeys(DATABASE, null, TABLE_PATTERN_UNDERSCORE)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getExportedKeys(DATABASE, SCHEMA, TABLE_PATTERN_PERCENT)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getExportedKeys(DATABASE, SCHEMA, TABLE_PATTERN_UNDERSCORE)));
    }

    @Test
    public void testPatternSearchEnabledForCrossReference() throws SQLException {
      setupForeignKeysMock(mockStatement);

      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA_PATTERN_PERCENT, TABLE1, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA_PATTERN_UNDERSCORE, TABLE1, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA_PATTERN_PERCENT, null, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA_PATTERN_UNDERSCORE, null, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA_PATTERN_PERCENT, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA_PATTERN_UNDERSCORE, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA_PATTERN_PERCENT, null)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA_PATTERN_UNDERSCORE, null)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, null, TABLE_PATTERN_PERCENT, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, null, TABLE_PATTERN_UNDERSCORE, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE_PATTERN_PERCENT, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE_PATTERN_UNDERSCORE, DATABASE, SCHEMA, TABLE2)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, null, TABLE_PATTERN_PERCENT)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, null, TABLE_PATTERN_UNDERSCORE)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA, TABLE_PATTERN_PERCENT)));
      assertEquals(
          1,
          getSizeOfResultSet(
              databaseMetaData.getCrossReference(
                  DATABASE, SCHEMA, TABLE1, DATABASE, SCHEMA, TABLE_PATTERN_UNDERSCORE)));
    }
  }
}
