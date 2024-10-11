package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;
import net.snowflake.client.core.SFSessionProperty;
import org.junit.jupiter.api.Test;

public class ConnectStringParseTest {
  @Test
  public void testParseAccountName() throws SnowflakeSQLException {
    Properties info = new Properties();
    info.setProperty("username", "test");
    String jdbcConnectString = "jdbc:snowflake://abc.us-east-1.snowflakecomputing.com";
    SnowflakeConnectString cstring = SnowflakeConnectString.parse(jdbcConnectString, info);
    assertThat(
        cstring.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey().toUpperCase()),
        is("abc"));

    // Hostname should be updated by default.
    jdbcConnectString = "jdbc:snowflake://abc_test.us-east-1.snowflakecomputing.com";
    cstring = SnowflakeConnectString.parse(jdbcConnectString, info);
    assertThat(
        cstring.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey().toUpperCase()),
        is("abc_test"));
    assertThat(cstring.getHost(), is("abc-test.us-east-1.snowflakecomputing.com"));

    jdbcConnectString = "jdbc:snowflake://abc-test.us-east-1.snowflakecomputing.com";
    cstring = SnowflakeConnectString.parse(jdbcConnectString, info);
    assertThat(
        cstring.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey().toUpperCase()),
        is("abc-test"));
    assertThat(cstring.getHost(), is("abc-test.us-east-1.snowflakecomputing.com"));

    //  Host name should be updated if the parameter is set and it has underscores in it.
    jdbcConnectString = "jdbc:snowflake://abc_test.us-east-1.snowflakecomputing.com";
    info.setProperty(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST.getPropertyKey(), "false");
    cstring = SnowflakeConnectString.parse(jdbcConnectString, info);
    assertThat(
        cstring.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey().toUpperCase()),
        is("abc_test"));
    assertThat(cstring.getHost(), is("abc-test.us-east-1.snowflakecomputing.com"));

    // No change if hostname does not have underscores in it.
    jdbcConnectString = "jdbc:snowflake://abc-test.us-east-1.snowflakecomputing.com";
    cstring = SnowflakeConnectString.parse(jdbcConnectString, info);
    assertThat(
        cstring.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey().toUpperCase()),
        is("abc-test"));
    assertThat(cstring.getHost(), is("abc-test.us-east-1.snowflakecomputing.com"));

    // The host URL should be updated whether the ACCOUNT property is set or not
    info.setProperty("ACCOUNT", "abc_test");
    cstring = SnowflakeConnectString.parse(jdbcConnectString, info);
    assertThat(
        cstring.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey().toUpperCase()),
        is("abc_test"));
    assertThat(cstring.getHost(), is("abc-test.us-east-1.snowflakecomputing.com"));
  }

  @Test
  public void testParseWithIllegalUriCharacters() {
    Properties info = new Properties();
    String jdbcConnectString =
        "jdbc:snowflake://abc-test.us-east-1.snowflakecomputing.com/?private_key_file=C:\\temp\\rsa_key.p8&private_key_file_pwd=test_password&user=test_user";
    SnowflakeConnectString cstring = SnowflakeConnectString.parse(jdbcConnectString, info);
    assertEquals("://:-1", cstring.toString());
  }
}
