package net.snowflake.client.jdbc.structuredtypes.sqldata;

import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.SFSqlInput;

public class NestedStructSqlData implements SQLData {

  private SimpleClass simpleClass;
  private List<SimpleClass> simpleClassses;
  private SimpleClass[] arrayOfSimpleClasses;
  private Map<String, SimpleClass> mapOfSimpleClasses;
  private List<String> texts;
  private Date[] arrayOfDates;
  private Map<String, Integer> mapOfIntegers;

  @Override
  public String getSQLTypeName() throws SQLException {
    return null;
  }

  @Override
  public void readSQL(SQLInput sqlInput, String typeName) throws SQLException {
    simpleClass = sqlInput.readObject(SimpleClass.class);
    simpleClassses = SFSqlInput.unwrap(sqlInput).readList(SimpleClass.class);
    arrayOfSimpleClasses = SFSqlInput.unwrap(sqlInput).readArray(SimpleClass.class);
    mapOfSimpleClasses = SFSqlInput.unwrap(sqlInput).readMap(SimpleClass.class);
    texts = SFSqlInput.unwrap(sqlInput).readList(String.class);
    arrayOfDates = SFSqlInput.unwrap(sqlInput).readArray(Date.class);
    mapOfIntegers = SFSqlInput.unwrap(sqlInput).readMap(Integer.class);
  }

  @Override
  public void writeSQL(SQLOutput stream) throws SQLException {}

  public SimpleClass getSimpleClass() {
    return simpleClass;
  }

  public List<SimpleClass> getSimpleClassses() {
    return simpleClassses;
  }

  public Map<String, SimpleClass> getMapOfSimpleClasses() {
    return mapOfSimpleClasses;
  }

  public List<String> getTexts() {
    return texts;
  }

  public Map<String, Integer> getMapOfIntegers() {
    return mapOfIntegers;
  }

  public SimpleClass[] getArrayOfSimpleClasses() {
    return arrayOfSimpleClasses;
  }

  public Date[] getArrayOfDates() {
    return arrayOfDates;
  }
}
