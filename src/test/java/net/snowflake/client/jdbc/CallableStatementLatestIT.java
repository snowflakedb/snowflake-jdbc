package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import net.snowflake.client.category.TestCategoryStatement;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryStatement.class)
public class CallableStatementLatestIT extends CallableStatementIT {

  public CallableStatementLatestIT(String format) {
    super(format);
  }

  /**
   * Test that function that removes curly brackets from outside of call statements works properly
   */
  @Test
  public void testParseSqlEscapeSyntaxFunction() {
    String[] callStatements = {
      "{call square_it(5)}", "call no_bracket_function(44)", "call {bracket_function(a=?)}"
    };
    String[] expectedStatements = {
      "call square_it(5)", "call no_bracket_function(44)", "call {bracket_function(a=?)}"
    };
    for (int i = 0; i < callStatements.length; i++) {
      assertEquals(
          expectedStatements[i],
          SnowflakeCallableStatementV1.parseSqlEscapeSyntax(callStatements[i]));
    }
  }

  /**
   * Test that prepareCall works the same as before with curly bracket syntax.
   *
   * @throws SQLException
   */
  @Test
  public void testPrepareCallWithCurlyBracketSyntax() throws SQLException {
    // test CallableStatement with no binding parameters
    connection = getConnection();
    statement = connection.createStatement();
    CallableStatement callableStatement = connection.prepareCall("{call square_it(5)}");
    assertThat(callableStatement.getParameterMetaData().getParameterCount(), is(0));

    // test CallableStatement with 1 binding parameter
    callableStatement = connection.prepareCall("{call square_it(?)}");
    // test that getParameterMetaData works with CallableStatement. At this point, it always returns
    // the type as "text."
    assertThat(callableStatement.getParameterMetaData().getParameterType(1), is(Types.VARCHAR));
    callableStatement.getParameterMetaData().getParameterTypeName(1);
    assertThat(callableStatement.getParameterMetaData().getParameterTypeName(1), is("text"));
    callableStatement.setFloat(1, 7.0f);
    ResultSet rs = callableStatement.executeQuery();
    rs.next();
    assertEquals(49.0f, rs.getFloat(1), 1.0f);

    // test CallableStatement with 2 binding parameters
    callableStatement = connection.prepareCall("{call add_nums(?,?)}");
    callableStatement.setDouble(1, 32);
    callableStatement.setDouble(2, 15);
    rs = callableStatement.executeQuery();
    rs.next();
    assertEquals(47, rs.getDouble(1), .5);
  }
}
