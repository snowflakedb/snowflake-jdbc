package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.category.TestCategoryArrow;
import net.snowflake.client.core.SFArrowResultSet;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryArrow.class)
public class ArrowBatchesTest extends BaseJDBCWithSharedConnectionIT {

  /** Necessary to conditional ignore tests */
  @Rule public ConditionalIgnoreRule rule = new ConditionalIgnoreRule();

  @BeforeClass
  public static void setUp() throws Exception {
    try (Statement statement = connection.createStatement()) {
      statement.execute("alter session set jdbc_query_result_format = 'arrow'");
      statement.execute("alter session set ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT = true");
      statement.execute(
          "alter session set FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT = true");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try (Statement statement = connection.createStatement()) {
      statement.execute("alter session unset jdbc_query_result_format");
      statement.execute("alter session unset ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT");
      statement.execute("alter session unset FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT");
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

  @Test
  public void testVarCharBatch() throws Exception {
    Statement statement = connection.createStatement();
    ResultSet rs =
        statement.executeQuery(
            "select 'Gallia est ' union select 'omnis divisa ' union select 'in partes tres';");
    ArrowBatches batches = rs.unwrap(SnowflakeResultSet.class).getArrowBatches();

    int totalRows = 0;
    List<Text> values = new ArrayList<>();

    while (batches.hasNext()) {
      ArrowBatch batch = batches.next();
      List<VectorSchemaRoot> roots = batch.fetch();
      for (VectorSchemaRoot root : roots) {
        totalRows += root.getRowCount();
        assertTrue(root.getVector(0) instanceof VarCharVector);
        VarCharVector vector = (VarCharVector) root.getVector(0);
        for (int i = 0; i < root.getRowCount(); i++) {
          values.add(vector.getObject(i));
        }
        root.close();
      }
    }
    assertNoMemoryLeaks(rs);
    rs.close();

    List<Text> expected =
        new ArrayList<Text>() {
          {
            add(new Text("Gallia est "));
            add(new Text("omnis divisa "));
            add(new Text("in partes tres"));
          }
        };

    assertTrue(values.containsAll(expected));

    assertEquals(3, totalRows);
  }

  private class Pair<A, B> {
    private final A first;
    private final B second;

    Pair(A first, B second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Pair)) {
        return false;
      }
      Pair<?, ?> other = (Pair<?, ?>) obj;
      return first.equals(other.first) && second.equals(other.second);
    }
  }

  @Test
  public void testStructBatch() throws Exception {
    Statement statement = connection.createStatement();
    ;
    ResultSet rs =
        statement.executeQuery(
            "select {'a': 3.1, 'b': 3.2}::object(a decimal(18, 3), b decimal(18, 3))"
                + " union select {'a': 2.2, 'b': 2.3}::object(a decimal(18, 3), b decimal(18, 3))");
    ArrowBatches batches = rs.unwrap(SnowflakeResultSet.class).getArrowBatches();

    int totalRows = 0;
    List<Pair<BigDecimal, BigDecimal>> values = new ArrayList<>();

    while (batches.hasNext()) {
      ArrowBatch batch = batches.next();
      List<VectorSchemaRoot> roots = batch.fetch();
      for (VectorSchemaRoot root : roots) {
        totalRows += root.getRowCount();
        assertTrue(root.getVector(0) instanceof StructVector);
        StructVector vector = (StructVector) root.getVector(0);
        DecimalVector aVector = (DecimalVector) vector.getChild("a");
        DecimalVector bVector = (DecimalVector) vector.getChild("b");
        for (int i = 0; i < root.getRowCount(); i++) {
          values.add(new Pair<>(aVector.getObject(i), bVector.getObject(i)));
        }
        root.close();
      }
    }
    assertNoMemoryLeaks(rs);
    rs.close();

    List<Pair<BigDecimal, BigDecimal>> expected =
        new ArrayList<Pair<BigDecimal, BigDecimal>>() {
          {
            add(new Pair<>(new BigDecimal("3.100"), new BigDecimal("3.200")));
            add(new Pair<>(new BigDecimal("2.200"), new BigDecimal("2.300")));
          }
        };

    assertTrue(values.containsAll(expected));

    assertEquals(2, totalRows);
  }

  @Test
  public void testListBatch() throws Exception {
    Statement statement = connection.createStatement();
    ResultSet rs =
        statement.executeQuery(
            "select array_construct(1.2, 2.3)::array(decimal(18, 3)) union all select array_construct(2.1, 1.0)::array(decimal(18, 3))");
    ArrowBatches batches = rs.unwrap(SnowflakeResultSet.class).getArrowBatches();

    int totalRows = 0;
    List<List<BigDecimal>> values = new ArrayList<>();

    while (batches.hasNext()) {
      ArrowBatch batch = batches.next();
      List<VectorSchemaRoot> roots = batch.fetch();
      for (VectorSchemaRoot root : roots) {
        totalRows += root.getRowCount();
        assertTrue(root.getVector(0) instanceof ListVector);
        ListVector vector = (ListVector) root.getVector(0);
        for (int i = 0; i < root.getRowCount(); i++) {
          values.add((List<BigDecimal>) vector.getObject(i));
        }
        root.close();
      }
    }
    assertNoMemoryLeaks(rs);
    rs.close();

    List<List<BigDecimal>> expected =
        new ArrayList<List<BigDecimal>>() {
          {
            add(
                new ArrayList<BigDecimal>() {
                  {
                    add(new BigDecimal("1.200"));
                    add(new BigDecimal("2.300"));
                  }
                });
            add(
                new ArrayList<BigDecimal>() {
                  {
                    add(new BigDecimal("2.100"));
                    add(new BigDecimal("1.000"));
                  }
                });
          }
        };

    assertTrue(expected.containsAll(values));

    assertEquals(2, totalRows);
  }

  @Test
  public void testMapBatch() throws Exception {
    Statement statement = connection.createStatement();
    ;
    ResultSet rs =
        statement.executeQuery(
            "select {'a': 3.1, 'b': 4.3}::map(varchar, decimal(18,3)) union"
                + " select {'c': 2.2, 'd': 1.5}::map(varchar, decimal(18,3))");
    ArrowBatches batches = rs.unwrap(SnowflakeResultSet.class).getArrowBatches();

    int totalRows = 0;
    List<Map<Text, BigDecimal>> values = new ArrayList<>();

    while (batches.hasNext()) {
      ArrowBatch batch = batches.next();
      List<VectorSchemaRoot> roots = batch.fetch();
      for (VectorSchemaRoot root : roots) {
        totalRows += root.getRowCount();
        assertTrue(root.getVector(0) instanceof MapVector);
        MapVector vector = (MapVector) root.getVector(0);
        VarCharVector keyVector =
            (VarCharVector) vector.getChildrenFromFields().get(0).getChildrenFromFields().get(0);
        DecimalVector valueVector =
            (DecimalVector) vector.getChildrenFromFields().get(0).getChildrenFromFields().get(1);
        for (int i = 0; i < root.getRowCount(); i++) {
          int startIndex = vector.getElementStartIndex(i);
          int endIndex = vector.getElementEndIndex(i);
          Map<Text, BigDecimal> map = new HashMap<>();
          for (int j = startIndex; j < endIndex; j++) {
            map.put(keyVector.getObject(j), valueVector.getObject(j));
          }
          values.add(map);
        }
        root.close();
      }
    }
    rs.close();

    // All expected values are present
    List<Map<Text, BigDecimal>> expected =
        Stream.of(
                new HashMap<Text, BigDecimal>() {
                  {
                    put(new Text("a"), new BigDecimal("3.100"));
                    put(new Text("b"), new BigDecimal("4.300"));
                  }
                },
                new HashMap<Text, BigDecimal>() {
                  {
                    put(new Text("c"), new BigDecimal("2.200"));
                    put(new Text("d"), new BigDecimal("1.500"));
                  }
                })
            .collect(Collectors.toList());

    assertTrue(values.containsAll(expected));

    assertEquals(2, totalRows);
  }

  @Test
  public void testFixedSizeListBatch() throws Exception {
    Statement statement = connection.createStatement();
    ResultSet rs =
        statement.executeQuery(
            "select [1, 2]::vector(int, 2) union all select [3, 4]::vector(int, 2)");
    ArrowBatches batches = rs.unwrap(SnowflakeResultSet.class).getArrowBatches();

    int totalRows = 0;
    List<List<Integer>> values = new ArrayList<>();

    while (batches.hasNext()) {
      ArrowBatch batch = batches.next();
      List<VectorSchemaRoot> roots = batch.fetch();
      for (VectorSchemaRoot root : roots) {
        totalRows += root.getRowCount();
        assertTrue(root.getVector(0) instanceof FixedSizeListVector);
        FixedSizeListVector vector = (FixedSizeListVector) root.getVector(0);
        for (int i = 0; i < root.getRowCount(); i++) {
          values.add((List<Integer>) vector.getObject(i));
        }
        root.close();
      }
    }
    assertNoMemoryLeaks(rs);
    rs.close();

    List<List<Integer>> expected =
        new ArrayList<List<Integer>>() {
          {
            add(
                new ArrayList<Integer>() {
                  {
                    add(1);
                    add(2);
                  }
                });
            add(
                new ArrayList<Integer>() {
                  {
                    add(3);
                    add(4);
                  }
                });
          }
        };

    assertTrue(expected.containsAll(values));

    assertEquals(2, totalRows);
  }
}
