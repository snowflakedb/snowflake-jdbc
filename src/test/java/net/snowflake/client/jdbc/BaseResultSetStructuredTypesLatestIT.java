package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryResultSet;
import net.snowflake.client.core.structs.SFSqlData;
import net.snowflake.client.core.structs.SFSqlInput;
import net.snowflake.client.core.structs.SFSqlOutput;
import net.snowflake.client.core.structs.SnowflakeObjectTypeFactories;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    private Boolean bool;
    private SimpleClass simpleClass;

    @Override
    public void readSql(SFSqlInput sqlInput) throws SQLException {
      string = sqlInput.readString("string");
      bool = sqlInput.readBoolean("bool");
      simpleClass = sqlInput.readObject("simpleClass", SimpleClass.class);
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
  public void testMapAllTypesOfFields() throws SQLException {
    SnowflakeObjectTypeFactories.register(AllTypesClass.class, AllTypesClass::new);
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select {" +
        "'string': 'a', " +
        "'bool': true, " +
        "'simpleClass': {'string': 'b'}" +
        "}::OBJECT(" +
        "string VARCHAR, " +
        "bool BOOLEAN, " +
        "simpleClass OBJECT(string VARCHAR)" +
        ")");
    resultSet.next();
    AllTypesClass object = resultSet.getObject(1, AllTypesClass.class);
    assertEquals("a", object.string);
    assertTrue(object.bool);
    assertEquals("b", object.simpleClass.string);
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
    Map<String, SimpleClass> map = resultSet.unwrap(SnowflakeBaseResultSet.class).getMap(1,  SimpleClass.class);
    assertEquals(map.get("x").string, "one");
    assertEquals(map.get("y").string, "two");
    statement.close();
    connection.close();
  }
}
