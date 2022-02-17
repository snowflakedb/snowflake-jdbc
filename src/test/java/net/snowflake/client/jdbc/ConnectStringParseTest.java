package net.snowflake.client.jdbc;

import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.jdbc.SnowflakeConnectString;
import java.util.Properties;

import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

public class ConnectStringParseTest {
    @Test
    public void testParseAccountName() throws  SnowflakeSQLException {
        Properties info = new Properties();
        info.setProperty("username", "test");
        String jdbcConnectString = "jdbc:snowflake://abc.us-east-1.snowflakecomputing.com";
        SnowflakeConnectString cstring = SnowflakeConnectString.parse(jdbcConnectString, info);
        assertThat(cstring.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey()), is("abc"));

        // Hostname should remain unchanged by default.
        jdbcConnectString = "jdbc:snowflake://abc_test.us-east-1.snowflakecomputing.com";
        cstring = SnowflakeConnectString.parse(jdbcConnectString, info);
        assertThat(cstring.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey()), is("abc_test"));
        assertThat(cstring.getHost(), is("abc_test.us-east-1.snowflakecomputing.com"));

        jdbcConnectString = "jdbc:snowflake://abc-test.us-east-1.snowflakecomputing.com";
        cstring = SnowflakeConnectString.parse(jdbcConnectString, info);
        assertThat(cstring.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey()), is("abc-test"));
        assertThat(cstring.getHost(), is("abc-test.us-east-1.snowflakecomputing.com"));

        //  Host name should be updated if the parameter is set and it has underscores in it.
        info.setProperty(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST.getPropertyKey(), "false");
        cstring = SnowflakeConnectString.parse(jdbcConnectString, info);
        assertThat(cstring.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey()), is("abc_test"));
        assertThat(cstring.getHost(), is("abc-test.us-east-1.snowflakecomputing.com"));

        // No change if hostname does not have underscores in it.
        jdbcConnectString = "jdbc:snowflake://abc-test.us-east-1.snowflakecomputing.com";
        cstring = SnowflakeConnectString.parse(jdbcConnectString, info);
        assertThat(cstring.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey()), is("abc-test"));
        assertThat(cstring.getHost(), is("abc-test.us-east-1.snowflakecomputing.com"));

        // The host URL should be updated whether the ACCOUNT property is set or not
        info.setProperty("ACCOUNT", "abc_test");
        cstring = SnowflakeConnectString.parse(jdbcConnectString, info);
        assertThat(cstring.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey()), is("abc_test"));
        assertThat(cstring.getHost(), is("abc-test.us-east-1.snowflakecomputing.com"));
    }
}
