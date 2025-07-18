package net.snowflake.client.wif;

import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(TestTags.WIF)
public class WIFLatestIT {

    private static final String ACCOUNT = System.getenv("SNOWFLAKE_TEST_WIF_ACCOUNT");
    private static final String HOST = System.getenv("SNOWFLAKE_TEST_WIF_HOST");

    @Test
    void shouldAuthenticateUsingWIFWithProviderDetection () {
        Properties properties = new Properties();
        properties.put("account", ACCOUNT);
        properties.put("authenticator", "WORKLOAD_IDENTITY");
        connectAndExecuteSimpleQuery(properties);
    }

    @Test
    void shouldAuthenticateUsingWIFWithExplicitProvider () {
        Properties properties = new Properties();
        properties.put("account", ACCOUNT);
        properties.put("authenticator", "WORKLOAD_IDENTITY");
        properties.put("workloadIdentityProvider", System.getenv("SNOWFLAKE_TEST_WIF_PROVIDER"));
        connectAndExecuteSimpleQuery(properties);
    }

    public void connectAndExecuteSimpleQuery(Properties props) {
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
}
