package net.snowflake.client.wif;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Running tests locally:
 *
 * <ol>
 *   <li>Push branch to repository
 *   <li>Set environment variables PARAMETERS_SECRET and BRANCH
 *   <li>Run ci/test_wif.sh
 * </ol>
 */
@Tag(TestTags.WIF)
public class WIFLatestIT {

  private static final String ACCOUNT = System.getenv("SNOWFLAKE_TEST_WIF_ACCOUNT");
  private static final String HOST = System.getenv("SNOWFLAKE_TEST_WIF_HOST");
  private static final String PROVIDER = System.getenv("SNOWFLAKE_TEST_WIF_PROVIDER");
  private static final String IS_GCP_FUNCTION = System.getenv("IS_GCP_FUNCTION");
  private static final String IMPERSONATION_PATH =
      System.getenv("SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH");
  private static final String IMPERSONATION_USER =
      System.getenv("SNOWFLAKE_TEST_WIF_USERNAME_IMPERSONATION");

  @Test
  void shouldAuthenticateUsingWIFWithDefinedProvider() {
    Properties properties = new Properties();
    properties.put("account", ACCOUNT);
    properties.put("authenticator", "WORKLOAD_IDENTITY");
    properties.put("workloadIdentityProvider", PROVIDER);
    connectAndExecuteSimpleQuery(properties);
  }

  @Test
  @DisabledIf("isProviderAzure")
  @EnabledIfEnvironmentVariable(named = "SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH", matches = ".+")
  void shouldAuthenticateUsingWIFWithImpersonation() {
    Properties properties = new Properties();
    properties.put("account", ACCOUNT);
    properties.put("authenticator", "WORKLOAD_IDENTITY");
    properties.put("workloadIdentityProvider", PROVIDER);
    properties.put("workloadIdentityImpersonationPath", IMPERSONATION_PATH);
    connectAndExecuteSimpleQuery(properties, IMPERSONATION_USER);
  }

  @Test
  @EnabledIf("isProviderGCP")
  void shouldAuthenticateUsingOIDC() {
    Assumptions.assumeTrue(!Objects.equals(IS_GCP_FUNCTION, "true"));
    Properties properties = new Properties();
    properties.put("account", ACCOUNT);
    properties.put("authenticator", "WORKLOAD_IDENTITY");
    properties.put("workloadIdentityProvider", "OIDC");
    properties.put("token", getGCPAccessToken());
    connectAndExecuteSimpleQuery(properties);
  }

  private static boolean isProviderGCP() {
    return Objects.equals(PROVIDER, "GCP");
  }

  private static boolean isProviderAzure() {
    return Objects.equals(PROVIDER, "AZURE");
  }

  private String getGCPAccessToken() {
    try {
      String command =
          "curl -H \"Metadata-Flavor: Google\" \"http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/identity?audience=snowflakecomputing.com\"";
      ProcessBuilder processBuilder = new ProcessBuilder("bash", "-c", command);
      Process process = processBuilder.start();

      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        String token = reader.readLine();
        int exitCode = process.waitFor();

        if (exitCode == 0 && token != null && !token.trim().isEmpty()) {
          return token.trim();
        } else {
          throw new RuntimeException("Failed to retrieve GCP access token, exit code: " + exitCode);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error executing GCP metadata request", e);
    }
  }

  private void connectAndExecuteSimpleQuery(Properties props) {
    String url = String.format("jdbc:snowflake://%s", HOST);
    try (Connection con = DriverManager.getConnection(url, props);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select 1")) {
      assertTrue(rs.next());
      int value = rs.getInt(1);
      assertEquals(1, value);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to execute query", e);
    }
  }

  private void connectAndExecuteSimpleQuery(Properties props, String expectedUser) {
    String url = String.format("jdbc:snowflake://%s", HOST);
    try (Connection con = DriverManager.getConnection(url, props);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select 1")) {
      assertTrue(rs.next());
      int value = rs.getInt(1);
      assertEquals(1, value);

      ResultSet rs2 = stmt.executeQuery("select current_user()");
      assertTrue(rs2.next());
      String username = rs2.getString(1);
      assertEquals(expectedUser, username);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to execute query", e);
    }
  }
}
