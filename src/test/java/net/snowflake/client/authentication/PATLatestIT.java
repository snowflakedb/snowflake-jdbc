package net.snowflake.client.authentication;

import static net.snowflake.client.authentication.AuthConnectionParameters.HOST;
import static net.snowflake.client.authentication.AuthConnectionParameters.SNOWFLAKE_INTERNAL_ROLE;
import static net.snowflake.client.authentication.AuthConnectionParameters.SNOWFLAKE_USER;
import static net.snowflake.client.authentication.AuthConnectionParameters.getOktaConnectionParameters;
import static net.snowflake.client.authentication.AuthConnectionParameters.getPATConnectionParameters;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.AUTHENTICATION)
public class PATLatestIT {

  AuthTestHelper authTestHelper;
  String patName;

  @BeforeEach
  public void setUp() throws IOException {
    authTestHelper = new AuthTestHelper();
  }

  @Test
  void shouldAuthenticateUsingPAT() {
    Properties properties = getPATConnectionParameters();
    properties.put("token", getPAT());
    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsNotThrown();
    removePAT();
  }

  @Test
  void shouldThrowErrorForInvalidPAT() {
    Properties properties = getPATConnectionParameters();
    properties.put("token", "invalidToken");
    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsThrown("Programmatic access token is invalid.");
  }

  @Test
  void shouldThrowErrorForMismatchedPATUsername() throws IOException {
    Properties properties = getPATConnectionParameters();
    properties.put("token", getPAT());
    properties.put("user", "differentUsername");
    authTestHelper.connectAndExecuteSimpleQuery(properties, null);
    authTestHelper.verifyExceptionIsThrown("Programmatic access token is invalid.");
    removePAT();
  }

  private String getPAT() {
    patName = "PAT_JDBC_" + generateRandomSuffix();
    String command =
        String.format(
            "alter user %s add programmatic access token %s ROLE_RESTRICTION = '%s'",
            SNOWFLAKE_USER, patName, SNOWFLAKE_INTERNAL_ROLE);
    return connectUsingDifferentAuthMethodAndExecuteCommand(command, true);
  }

  private void removePAT() {
    String command =
        String.format(
            "alter user %s remove programmatic access token %s;", SNOWFLAKE_USER, patName);
    connectUsingDifferentAuthMethodAndExecuteCommand(command, false);
  }

  private String connectUsingDifferentAuthMethodAndExecuteCommand(
      String command, boolean shouldReturnToken) {
    Properties properties = getOktaConnectionParameters();
    String url = String.format("jdbc:snowflake://%s", HOST);

    try (Connection con = DriverManager.getConnection(url, properties);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(command)) {
      if (shouldReturnToken && rs.next()) {
        return rs.getString("token_secret");
      }
      return null;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private String generateRandomSuffix() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    return sdf.format(new Date());
  }
}
