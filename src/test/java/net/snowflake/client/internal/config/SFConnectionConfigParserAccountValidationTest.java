package net.snowflake.client.internal.config;

import static net.snowflake.client.internal.config.SFConnectionConfigParser.SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY;
import static net.snowflake.client.internal.config.SFConnectionConfigParser.SNOWFLAKE_HOME_KEY;
import static net.snowflake.client.internal.jdbc.SnowflakeUtil.isWindows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.SFSessionProperty;
import net.snowflake.client.internal.jdbc.SnowflakeConnectString;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers the account/port/protocol validation added to {@link SFConnectionConfigParser#createUrl},
 * the only place in the driver that synthesizes a network authority from {@code account}.
 *
 * <p>Assertions are on the FINAL PARSED AUTHORITY (what {@link SnowflakeConnectString} resolves the
 * generated connect string to), not merely on the generated text, because that authority is what a
 * credential-bearing login is actually sent to.
 *
 * <p>There is intentionally no test for a '/' in the account: {@link SnowflakeConnectString#parse}
 * already rejects a connect string with a non-empty path, so a passing '/' case would prove nothing
 * about this validation.
 */
public class SFConnectionConfigParserAccountValidationTest {

  private Path tempPath;
  private final TomlMapper tomlMapper = new TomlMapper();
  private final Map<String, String> savedEnv = new HashMap<>();

  @BeforeEach
  public void setUp() throws IOException {
    for (String key : new String[] {SNOWFLAKE_HOME_KEY, SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY}) {
      if (SnowflakeUtil.systemGetEnv(key) != null) {
        savedEnv.put(key, SnowflakeUtil.systemGetEnv(key));
      }
    }
    tempPath = Files.createTempDirectory(".snowflake");
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_HOME_KEY, tempPath.toString());
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "default");
  }

  @AfterEach
  public void tearDown() throws IOException {
    SnowflakeUtil.systemUnsetEnv(SNOWFLAKE_HOME_KEY);
    SnowflakeUtil.systemUnsetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY);
    Files.walk(tempPath).map(Path::toFile).forEach(File::delete);
    Files.deleteIfExists(tempPath);
    savedEnv.forEach(SnowflakeUtil::systemSetEnv);
  }

  // ---------------------------------------------------------------------------------------------
  // Compatibility: every real account form must still resolve to its own authority
  // ---------------------------------------------------------------------------------------------

  @ParameterizedTest
  @CsvSource({
    // bare account locator
    "xy12345, xy12345.snowflakecomputing.com",
    // org-account form with a hyphen
    "myorg-myaccount, myorg-myaccount.snowflakecomputing.com",
    // dotted regional form
    "acct.us-east-1, acct.us-east-1.snowflakecomputing.com",
    // multi-label dotted form
    "a.b.c, a.b.c.snowflakecomputing.com",
    // consecutive hyphens, accepted by Python/.NET/libsfc/UD
    "a--b, a--b.snowflakecomputing.com",
    // the form used by the pre-existing tests in this package
    "snowaccount.us-west-2.aws, snowaccount.us-west-2.aws.snowflakecomputing.com",
    // digits-only label
    "12345, 12345.snowflakecomputing.com",
  })
  public void testValidAccountFormsResolveToTheirOwnAuthority(String account, String expectedHost)
      throws Exception {
    writeToml(account, null, null);

    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals("jdbc:snowflake://" + expectedHost + ":443", data.getUrl());
    assertEquals(expectedHost + ":443", parsedAuthority(data.getUrl(), new Properties()));
  }

  /**
   * Underscores must keep working. JDBC's documented behaviour is that the derived host has its
   * underscores rewritten to hyphens unless allowUnderscoresInHost=true
   * (SnowflakeConnectString:146-166); both branches must survive validation.
   */
  @Test
  public void testUnderscoreAccountIsAcceptedAndKeepsExistingHostRewriteBehaviour()
      throws Exception {
    writeToml("my_acct", null, null);

    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals("jdbc:snowflake://my_acct.snowflakecomputing.com:443", data.getUrl());

    // default: underscores in the host are rewritten to hyphens
    assertEquals(
        "my-acct.snowflakecomputing.com:443", parsedAuthority(data.getUrl(), new Properties()));

    // ALLOW_UNDERSCORES_IN_HOST=true: the underscore is preserved
    Properties allowUnderscores = new Properties();
    allowUnderscores.setProperty(
        SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST.getPropertyKey(), "true");
    assertEquals(
        "my_acct.snowflakecomputing.com:443", parsedAuthority(data.getUrl(), allowUnderscores));
  }

  /** An explicit host is unaffected: no account-derived authority is synthesized. */
  @Test
  public void testExplicitHostStillWins() throws Exception {
    Map<String, Object> extra = new HashMap<>();
    extra.put("host", "snowflake.reg.local");
    writeToml("myorg-myaccount", "8082", null, extra);

    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals("jdbc:snowflake://snowflake.reg.local:8082", data.getUrl());
    assertEquals("snowflake.reg.local:8082", parsedAuthority(data.getUrl(), new Properties()));
  }

  // ---------------------------------------------------------------------------------------------
  // The vector: percent-encoded delimiters arriving through the auto-configuration URL query
  // ---------------------------------------------------------------------------------------------

  /**
   * Establishes that the escape is real and that the assertions below are meaningful: this is
   * exactly the text {@code String.format("jdbc:snowflake://%s:%s", ...)} produced for an
   * unvalidated account of "other.example.com?x", and the authority it resolves to is the
   * attacker's host, not a snowflakecomputing.com host.
   */
  @Test
  public void testUnvalidatedAccountWouldResolveToAttackerAuthority() {
    assertEquals(
        "other.example.com:443",
        parsedAuthority(
            "jdbc:snowflake://other.example.com?x.snowflakecomputing.com:443", new Properties()));
    assertEquals(
        "other.example.com:443",
        parsedAuthority(
            "jdbc:snowflake://other.example.com#x.snowflakecomputing.com:443", new Properties()));
  }

  /**
   * The capability-satisfying vector: {@code parseAutoConfigJdbcUrlParameters} URL-decodes values,
   * so "%3F" becomes a raw '?' by the time it reaches createUrl. The same encoding is what stops an
   * attacker from simply injecting "&host=", which is why this specific shape matters.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "other.example.com%3Fx", // -> other.example.com?x
        "other.example.com%3F", // -> other.example.com?
        "other.example.com%23x", // -> other.example.com#x
        "other.example.com%23", // -> other.example.com#
      })
  public void testPercentEncodedDelimiterInAccountFromAutoConfigUrlIsRejected(String encodedAccount)
      throws Exception {
    writeToml("myorg-myaccount", null, null);

    SnowflakeSQLException e =
        assertThrows(
            SnowflakeSQLException.class,
            () ->
                SFConnectionConfigParser.buildConnectionParameters(
                    "jdbc:snowflake:auto?connectionName=default&account=" + encodedAccount));
    assertTrue(
        e.getMessage().contains("Invalid account"),
        "expected an account validation failure, got: " + e.getMessage());
  }

  /** The account never even reaches the OAuth token file read, which happens after createUrl. */
  @Test
  public void testRejectionHappensBeforeTokenFileIsRead() throws Exception {
    Path tokenFile = Paths.get(tempPath.toString(), "unreadable-token");
    Map<String, Object> extra = new HashMap<>();
    extra.put("authenticator", "oauth");
    extra.put("token_file_path", tokenFile.toString());
    writeToml("myorg-myaccount", null, null, extra);

    // token file deliberately absent: if validation ran late we would see a token-read failure
    SnowflakeSQLException e =
        assertThrows(
            SnowflakeSQLException.class,
            () ->
                SFConnectionConfigParser.buildConnectionParameters(
                    "jdbc:snowflake:auto?connectionName=default&account=other.example.com%3Fx"));
    assertTrue(
        e.getMessage().contains("Invalid account"),
        "expected an account validation failure, got: " + e.getMessage());
  }

  // ---------------------------------------------------------------------------------------------
  // Raw delimiters and other rejected account shapes, straight from the TOML
  // ---------------------------------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(
      strings = {
        "other.example.com?x", // raw '?' terminates the authority
        "other.example.com#x", // raw '#' terminates the authority
        "other.example.com:8080", // raw ':' breaks the host/port split
        "user@other.example.com", // userinfo delimiter
        "other.example.com\\x", // backslash
        "other example com", // whitespace
        "acct.", // trailing empty label
        ".acct", // leading empty label
        "a..b", // empty middle label
        "acct%2Ex", // percent escape is not a label separator
        "acct+x",
      })
  public void testRejectedAccountShapesFromToml(String account) throws Exception {
    writeToml(account, null, null);

    SnowflakeSQLException e =
        assertThrows(
            SnowflakeSQLException.class,
            () -> SFConnectionConfigParser.buildConnectionParameters(""));
    assertTrue(
        e.getMessage().contains("Invalid account"),
        "expected an account validation failure, got: " + e.getMessage());
  }

  // ---------------------------------------------------------------------------------------------
  // port / protocol
  // ---------------------------------------------------------------------------------------------

  @ParameterizedTest
  @CsvSource({
    "8082, jdbc:snowflake://myorg-myaccount.snowflakecomputing.com:8082",
    "1, jdbc:snowflake://myorg-myaccount.snowflakecomputing.com:1",
    "65535, jdbc:snowflake://myorg-myaccount.snowflakecomputing.com:65535",
  })
  public void testValidPortsAreAccepted(String port, String expectedUrl) throws Exception {
    writeToml("myorg-myaccount", port, null);
    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals(expectedUrl, data.getUrl());
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "70000", "-1", "4a3", "443&x", "443 ", "443.0", "0443443"})
  public void testInvalidPortsAreRejected(String port) throws Exception {
    writeToml("myorg-myaccount", port, null);
    SnowflakeSQLException e =
        assertThrows(
            SnowflakeSQLException.class,
            () -> SFConnectionConfigParser.buildConnectionParameters(""));
    assertTrue(
        e.getMessage().contains("Invalid port"),
        "expected a port validation failure, got: " + e.getMessage());
  }

  @ParameterizedTest
  @CsvSource({
    "http, jdbc:snowflake://http://myorg-myaccount.snowflakecomputing.com:80",
    "HTTP, jdbc:snowflake://http://myorg-myaccount.snowflakecomputing.com:80",
    "https, jdbc:snowflake://myorg-myaccount.snowflakecomputing.com:443",
    "HTTPS, jdbc:snowflake://myorg-myaccount.snowflakecomputing.com:443",
  })
  public void testValidProtocolsAreAccepted(String protocol, String expectedUrl) throws Exception {
    writeToml("myorg-myaccount", null, protocol);
    ConnectionParameters data = SFConnectionConfigParser.buildConnectionParameters("");
    assertNotNull(data);
    assertEquals(expectedUrl, data.getUrl());
  }

  @ParameterizedTest
  @ValueSource(strings = {"ftp", "htpps", "file", "http://"})
  public void testInvalidProtocolsAreRejected(String protocol) throws Exception {
    writeToml("myorg-myaccount", null, protocol);
    SnowflakeSQLException e =
        assertThrows(
            SnowflakeSQLException.class,
            () -> SFConnectionConfigParser.buildConnectionParameters(""));
    assertTrue(
        e.getMessage().contains("Invalid protocol"),
        "expected a protocol validation failure, got: " + e.getMessage());
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  /** Resolves a generated connect string the way the driver does and returns "host:port". */
  private static String parsedAuthority(String url, Properties info) {
    SnowflakeConnectString cs = SnowflakeConnectString.parse(url, info);
    assertTrue(cs.isValid(), "connect string did not parse: " + url);
    return cs.getHost() + ":" + cs.getPort();
  }

  private void writeToml(String account, String port, String protocol) throws IOException {
    writeToml(account, port, protocol, null);
  }

  private void writeToml(
      String account, String port, String protocol, Map<String, Object> extraParams)
      throws IOException {
    Path path = Paths.get(tempPath.toString(), "connections.toml");
    Files.deleteIfExists(path);
    File file = createOwnerOnlyFile(path).toFile();

    Map<String, Object> params = new HashMap<>();
    params.put("account", account);
    params.put("user", "TOML_USER");
    params.put("password", "TOML_PASS");
    if (port != null) {
      params.put("port", port);
    }
    if (protocol != null) {
      params.put("protocol", protocol);
    }
    if (extraParams != null) {
      params.putAll(extraParams);
    }

    Map<String, Object> configuration = new HashMap<>();
    configuration.put("default", params);
    tomlMapper.writeValue(file, configuration);
  }

  private Path createOwnerOnlyFile(Path path) throws IOException {
    if (isWindows()) {
      return Files.createFile(path);
    }
    FileAttribute<Set<PosixFilePermission>> attribute =
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"));
    return Files.createFile(path, attribute);
  }
}
