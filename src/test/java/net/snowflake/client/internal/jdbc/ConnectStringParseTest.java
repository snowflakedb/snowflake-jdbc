package net.snowflake.client.internal.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.SFSessionProperty;
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

    //  Host name is normalized to the hyphenated form Snowflake serves, while the account
    //  identifier keeps its underscores.
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

  /**
   * allowUnderscoresInHost is the escape hatch for deployments whose DNS only resolves the
   * underscored host, such as some PrivateLink setups. When it is set, the host must be left
   * exactly as the connect string gave it.
   */
  @Test
  public void testAllowUnderscoresInHostPreservesHost() {
    Properties info = new Properties();
    info.setProperty("username", "test");
    info.setProperty(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST.getPropertyKey(), "true");

    SnowflakeConnectString cstring =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://abc_test.us-east-1.snowflakecomputing.com", info);
    assertThat(
        cstring.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey().toUpperCase()),
        is("abc_test"));
    assertThat(cstring.getHost(), is("abc_test.us-east-1.snowflakecomputing.com"));

    cstring =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://abc_test.privatelink.snowflakecomputing.com", info);
    assertThat(cstring.getHost(), is("abc_test.privatelink.snowflakecomputing.com"));
  }

  /**
   * The flag defaults to false, so an underscored Snowflake host is normalized when it is absent.
   */
  @Test
  public void testHostIsNormalizedWhenAllowUnderscoresInHostIsAbsent() {
    Properties info = new Properties();
    info.setProperty("username", "test");

    SnowflakeConnectString cstring =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://abc_test.privatelink.snowflakecomputing.com", info);
    assertThat(cstring.getHost(), is("abc-test.privatelink.snowflakecomputing.com"));
  }

  /**
   * Only Snowflake is known to serve a hyphenated variant of an underscored host, so a
   * non-Snowflake host is never rewritten regardless of the flag.
   */
  @Test
  public void testNonSnowflakeHostIsNeverNormalized() {
    Properties info = new Properties();
    info.setProperty("username", "test");
    info.setProperty("account", "abc_test");

    SnowflakeConnectString cstring =
        SnowflakeConnectString.parse("jdbc:snowflake://abc_test.internal.example.com", info);
    assertThat(cstring.getHost(), is("abc_test.internal.example.com"));
  }

  /**
   * The host does not have to contain the account identifier at all - it may be an IP address, or
   * some other name that merely routes to the account. Rewriting a host we cannot tie to the
   * account would point the driver at a name Snowflake never promised to serve, so leave those
   * alone.
   */
  @Test
  public void testHostNotStartingWithAccountIsNotNormalized() {
    Properties info = new Properties();
    info.setProperty("username", "test");
    info.setProperty("account", "abc_test");

    SnowflakeConnectString cstring =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://some_gateway.us-east-1.snowflakecomputing.com", info);
    assertThat(cstring.getHost(), is("some_gateway.us-east-1.snowflakecomputing.com"));
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
