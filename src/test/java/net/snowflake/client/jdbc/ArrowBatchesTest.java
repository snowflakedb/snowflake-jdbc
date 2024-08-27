package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import net.snowflake.client.core.SFArrowResultSet;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ArrowBatchesTest extends BaseJDBCWithSharedConnectionIT {
  @Before
  public void setUp() throws Exception {
    try (Statement statement = connection.createStatement()) {
      statement.execute("alter session set jdbc_query_result_format = 'arrow'");
    }
  }

  @After
  public void tearDown() throws Exception {
    try (Statement statement = connection.createStatement()) {
      statement.execute("alter session unset jdbc_query_result_format");
    }
  }

  private static void assertNoMemoryLeaks(ResultSet rs) throws SQLException {
    assertEquals(
        ((SFArrowResultSet) rs.unwrap(SnowflakeResultSetV1.class).sfBaseResultSet)
            .getAllocatedMemory(),
        0);
  }

  @Test
  public void testMultipleBatches() throws Exception {
    Statement statement = connection.createStatement();
    ResultSet rs =
        statement.executeQuery(
            "select seq1(), seq2(), seq4(), seq8() from TABLE (generator(rowcount => 300000))");
    ArrowBatches batches = rs.unwrap(SnowflakeResultSet.class).getArrowBatches();
    assertEquals(batches.getRowCount(), 300000);
    int totalRows = 0;
    ArrayList<VectorSchemaRoot> allRoots = new ArrayList<>();
    while (batches.hasNext()) {
      ArrowBatch batch = batches.next();
      List<VectorSchemaRoot> roots = batch.fetch();
      for (VectorSchemaRoot root : roots) {
        totalRows += root.getRowCount();
        allRoots.add(root);
        assertTrue(root.getVector(0) instanceof TinyIntVector);
        assertTrue(root.getVector(1) instanceof SmallIntVector);
        assertTrue(root.getVector(2) instanceof IntVector);
        assertTrue(root.getVector(3) instanceof BigIntVector);
      }
    }

    rs.close();

    // The memory should not be freed when closing the result set.
    for (VectorSchemaRoot root : allRoots) {
      assertTrue(root.getVector(0).getValueCount() > 0);
      root.close();
    }
    assertNoMemoryLeaks(rs);
    assertEquals(300000, totalRows);
  }

  @Test
  public void testTinyIntBatch() throws Exception {
    Statement statement = connection.createStatement();
    ;
    ResultSet rs = statement.executeQuery("select 1 union select 2 union select 3;");
    ArrowBatches batches = rs.unwrap(SnowflakeResultSet.class).getArrowBatches();

    int totalRows = 0;
    List<Byte> values = new ArrayList<>();

    while (batches.hasNext()) {
      ArrowBatch batch = batches.next();
      List<VectorSchemaRoot> roots = batch.fetch();
      for (VectorSchemaRoot root : roots) {
        totalRows += root.getRowCount();
        assertTrue(root.getVector(0) instanceof TinyIntVector);
        TinyIntVector vector = (TinyIntVector) root.getVector(0);
        for (int i = 0; i < root.getRowCount(); i++) {
          values.add(vector.get(i));
        }
        root.close();
      }
    }
    rs.close();

    // All expected values are present
    for (byte i = 1; i < 4; i++) {
      assertTrue(values.contains(i));
    }

    assertEquals(3, totalRows);
  }

  @Test
  public void testSmallIntBatch() throws Exception {
    Statement statement = connection.createStatement();
    ;
    ResultSet rs = statement.executeQuery("select 129 union select 130 union select 131;");
    ArrowBatches batches = rs.unwrap(SnowflakeResultSet.class).getArrowBatches();

    int totalRows = 0;
    List<Short> values = new ArrayList<>();

    while (batches.hasNext()) {
      ArrowBatch batch = batches.next();
      List<VectorSchemaRoot> roots = batch.fetch();
      for (VectorSchemaRoot root : roots) {
        totalRows += root.getRowCount();
        assertTrue(root.getVector(0) instanceof SmallIntVector);
        SmallIntVector vector = (SmallIntVector) root.getVector(0);
        for (int i = 0; i < root.getRowCount(); i++) {
          values.add(vector.get(i));
        }
        root.close();
      }
    }
    rs.close();

    // All expected values are present
    for (short i = 129; i < 132; i++) {
      assertTrue(values.contains(i));
    }

    assertEquals(3, totalRows);
  }

  @Test
  public void testIntBatch() throws Exception {
    Statement statement = connection.createStatement();
    ;
    ResultSet rs = statement.executeQuery("select 100000 union select 100001 union select 100002;");
    ArrowBatches batches = rs.unwrap(SnowflakeResultSet.class).getArrowBatches();

    int totalRows = 0;
    List<Integer> values = new ArrayList<>();

    while (batches.hasNext()) {
      ArrowBatch batch = batches.next();
      List<VectorSchemaRoot> roots = batch.fetch();
      for (VectorSchemaRoot root : roots) {
        totalRows += root.getRowCount();
        assertTrue(root.getVector(0) instanceof IntVector);
        IntVector vector = (IntVector) root.getVector(0);
        for (int i = 0; i < root.getRowCount(); i++) {
          values.add(vector.get(i));
        }
        root.close();
      }
    }
    rs.close();

    // All expected values are present
    for (int i = 100000; i < 100003; i++) {
      assertTrue(values.contains(i));
    }

    assertEquals(3, totalRows);
  }

  @Test
  public void testBigIntBatch() throws Exception {
    Statement statement = connection.createStatement();
    ;
    ResultSet rs =
        statement.executeQuery(
            "select 10000000000 union select 10000000001 union select 10000000002;");
    ArrowBatches batches = rs.unwrap(SnowflakeResultSet.class).getArrowBatches();

    int totalRows = 0;
    List<Long> values = new ArrayList<>();

    while (batches.hasNext()) {
      ArrowBatch batch = batches.next();
      List<VectorSchemaRoot> roots = batch.fetch();
      for (VectorSchemaRoot root : roots) {
        totalRows += root.getRowCount();
        assertTrue(root.getVector(0) instanceof BigIntVector);
        BigIntVector vector = (BigIntVector) root.getVector(0);
        for (int i = 0; i < root.getRowCount(); i++) {
          values.add(vector.get(i));
        }
        root.close();
      }
    }
    rs.close();

    // All expected values are present
    for (long i = 10000000000L; i < 10000000003L; i++) {
      assertTrue(values.contains(i));
    }

    assertEquals(3, totalRows);
  }

  @Test
  public void testDecimalBatch() throws Exception {
    Statement statement = connection.createStatement();
    ;
    ResultSet rs = statement.executeQuery("select 1.1 union select 1.2 union select 1.3;");
    ArrowBatches batches = rs.unwrap(SnowflakeResultSet.class).getArrowBatches();

    int totalRows = 0;
    List<BigDecimal> values = new ArrayList<>();

    while (batches.hasNext()) {
      ArrowBatch batch = batches.next();
      List<VectorSchemaRoot> roots = batch.fetch();
      for (VectorSchemaRoot root : roots) {
        totalRows += root.getRowCount();
        assertTrue(root.getVector(0) instanceof DecimalVector);
        DecimalVector vector = (DecimalVector) root.getVector(0);
        for (int i = 0; i < root.getRowCount(); i++) {
          values.add(vector.getObject(i));
        }
        root.close();
      }
    }

    rs.close();

    // All expected values are present
    for (int i = 1; i < 4; i++) {
      assertTrue(values.contains(new BigDecimal("1." + i)));
    }

    assertEquals(3, totalRows);
  }
}
