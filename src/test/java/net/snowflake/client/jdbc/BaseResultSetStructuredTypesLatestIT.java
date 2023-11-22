package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryResultSet;
import net.snowflake.client.core.structs.SFSqlData;
import net.snowflake.client.core.structs.SFSqlInput;
import net.snowflake.client.core.structs.SFSqlOutput;
import net.snowflake.client.core.structs.SnowflakeObjectTypeFactories;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(TestCategoryResultSet.class)
public abstract class BaseResultSetStructuredTypesLatestIT {
  private final String queryResultFormat;

  protected BaseResultSetStructuredTypesLatestIT(String queryResultFormat) {
    this.queryResultFormat = queryResultFormat;
  }

  public Connection init() throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    Statement stmt = conn.createStatement();
    stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    stmt.close();
    return conn;
  }

  public static class SimpleClass implements SFSqlData {

    private String string;

    public SimpleClass(String string) {
      this.string = string;
    }
    public SimpleClass() {}

    @Override
    public void readSql(SFSqlInput sqlInput) throws SQLException {
      string = sqlInput.readString("string");
    }

    @Override
    public void writeSql(SFSqlOutput sqlOutput) throws SQLException {
      sqlOutput.writeString("string", string);
    }
  }

  public static class AllTypesClass implements SFSqlData {

    private String string;
    private Byte b;
    private Short s;
    private Integer i;
    private Long l;
    private Float f;
    private Double d;
    private Boolean bool;
    private SimpleClass simpleClass;

    @Override
    public void readSql(SFSqlInput sqlInput) throws SQLException {
      string = sqlInput.readString("string");
      b = sqlInput.readByte("b");
      s = sqlInput.readShort("s");
      i = sqlInput.readInt("i");
      l = sqlInput.readLong("l");
      f = sqlInput.readFloat("f");
      d = sqlInput.readDouble("d");
      bool = sqlInput.readBoolean("bool");
      simpleClass = sqlInput.readObject("simpleClass", SimpleClass.class);
    }

    @Override
    public void writeSql(SFSqlOutput sqlOutput) throws SQLException {

    }
  }

  public static class AllStructuredTypesClass implements SFSqlData {

    private SimpleClass simpleClass;
    private List<SimpleClass> simpleClassList;
    private SimpleClass[] simpleClassArray;
    private Map<String, SimpleClass> simpleClassesMap;


    @Override
    public void readSql(SFSqlInput sqlInput) throws SQLException {
      simpleClass = sqlInput.readObject("simpleClass", SimpleClass.class);
      simpleClassList = sqlInput.readList("simpleClassList", SimpleClass.class);
      simpleClassArray = sqlInput.readArray("simpleClassArray", SimpleClass.class);
      simpleClassesMap = sqlInput.readMap("simpleClassMap", String.class, SimpleClass.class);
    }

    @Override
    public void writeSql(SFSqlOutput sqlOutput) throws SQLException {
    }
  }

  @Test
  public void testMapStructToObjectWithFactory() throws SQLException {
    testMapJson(true);
  }

  @Test
  public void testMapStructToObjectWithReflection() throws SQLException {
    testMapJson(false);
  }

  private void testMapJson(boolean registerFactory) throws SQLException {
    if (registerFactory) {
      SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    } else {
      SnowflakeObjectTypeFactories.unregister(SimpleClass.class);
    }
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select {'string':'a'}::OBJECT(string VARCHAR)");
    resultSet.next();
    SimpleClass object = resultSet.getObject(1, SimpleClass.class);
    assertEquals("a", object.string);
    statement.close();
    connection.close();
  }
  @Test
  public void testInsert() throws SQLException {
    testInsertObject();
  }
  private void testInsertObject() throws SQLException {

    Connection connection = init();
    PreparedStatement statement = connection.prepareStatement("INSERT INTO structs2 (struct) SELECT parse_json(?) :: OBJECT(string VARCHAR) ");

    SimpleClass simpleClass = new SimpleClass("AAAAA");

    statement.setObject(1, simpleClass);
    statement.execute();
    ResultSet resultSet = statement.executeQuery("select struct from structs2");
    resultSet.next();
    SimpleClass object = resultSet.getObject(1, SimpleClass.class);
    assertEquals("AAAAA", object.string);
    statement.close();
    connection.close();
  }

  @Test
  public void testMapAllTypesOfFields() throws SQLException {
    SnowflakeObjectTypeFactories.register(AllTypesClass.class, AllTypesClass::new);
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select {" +
        "'string': 'a', " +
        "'b': 1, " +
        "'s': 2, " +
        "'i': 3, " +
        "'l': 4, " +
        "'f': 1.1, " +
        "'d': 2.2, " +
        "'bool': true, " +
        "'simpleClass': {'string': 'b'}" +
        "}::OBJECT(" +
        "string VARCHAR, " +
        "b TINYINT, " +
        "s SMALLINT, " +
        "i INTEGER, " +
        "l BIGINT, " +
        "f FLOAT, " +
        "d DOUBLE, " +
        "bool BOOLEAN, " +
        "simpleClass OBJECT(string VARCHAR)" +
        ")");
    resultSet.next();
    AllTypesClass object = resultSet.getObject(1, AllTypesClass.class);
    assertEquals("a", object.string);
    assertEquals(1, (long) object.b);
    assertEquals(2, (long) object.s);
    assertEquals(3, (long) object.i);
    assertEquals(4, (long) object.l);
    assertEquals(1.1, (double) object.f, 0.01);
    assertEquals(2.2, (double) object.d, 0.01);
    assertTrue(object.bool);
    assertEquals("b", object.simpleClass.string);
    statement.close();
    connection.close();
  }
  @Test
  public void testMapAllStructuredTypes() throws SQLException {
    SnowflakeObjectTypeFactories.register(AllStructuredTypesClass.class, AllStructuredTypesClass::new);
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select {" +
        "'simpleClass': {'string': 'b'}, " +
        "'simpleClassList': [{'string': 'c'}, {'string': 'd'}] ," +
        "'simpleClassArray': [{'string': 'c1'}, {'string': 'd1'}] ," +
        "'simpleClassMap': {'x':{'string':'e'},'y':{'string':'f'}}" +
        "}::OBJECT(" +
        "simpleClass OBJECT(string VARCHAR), " +
        "simpleClassList ARRAY(OBJECT(string VARCHAR)), " +
        "simpleClassArray ARRAY(OBJECT(string VARCHAR)), " +
        "simpleClassMap MAP(VARCHAR, OBJECT(string VARCHAR))" +
        ")");
    resultSet.next();
    AllStructuredTypesClass object = resultSet.getObject(1, AllStructuredTypesClass.class);
    assertEquals("b", object.simpleClass.string);
    assertEquals("c", (object.simpleClassList.get(0)).string);
    assertEquals("d", (object.simpleClassList.get(1)).string);
    assertEquals("c1", (object.simpleClassArray[0]).string);
    assertEquals("d1", (object.simpleClassArray[1]).string);
    assertEquals("e", (object.simpleClassesMap.get("x")).string);
    assertEquals("f", (object.simpleClassesMap.get("y")).string);
    statement.close();
    connection.close();
  }

  @Test
  public void testMapStructsFromChunks() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "select {'string':'a'}::OBJECT(string VARCHAR) FROM TABLE(GENERATOR(ROWCOUNT=>30000))");
    int i = 0;
    while (resultSet.next()) {
      SimpleClass object = resultSet.getObject(1, SimpleClass.class);
      assertEquals("a", object.string);
    }
    statement.close();
    connection.close();
  }

  @Test
  public void testMapList() throws SQLException {
    SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select [{'string':'aaa'},{'string': 'bbb'}]::ARRAY(OBJECT(string varchar))");
    resultSet.next();
    List<SimpleClass> objects = resultSet.unwrap(SnowflakeBaseResultSet.class).getList(1,  SimpleClass.class);
    assertEquals(objects.get(0).string, "aaa");
    assertEquals(objects.get(1).string, "bbb");
    statement.close();
    connection.close();
  }

  @Test
  public void testMapArray() throws SQLException {
    SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select [{'string':'aaa'},{'string': 'bbb'}]::ARRAY(OBJECT(string varchar))");
    resultSet.next();
    SimpleClass[] objects = resultSet.unwrap(SnowflakeBaseResultSet.class).getArray(1,  SimpleClass.class);
    assertEquals(objects[0].string, "aaa");
    assertEquals(objects[1].string, "bbb");

    statement.close();
    connection.close();
  }

  @Test
  public void testMapMap() throws SQLException {
    SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select {'x':{'string':'one'},'y':{'string':'two'}}::MAP(VARCHAR, OBJECT(string VARCHAR))");
    resultSet.next();
    Map<String, SimpleClass> map = resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1, String.class, SimpleClass.class);
    assertEquals(map.get("x").string, "one");
    assertEquals(map.get("y").string, "two");
    statement.close();
    connection.close();
  }
}
