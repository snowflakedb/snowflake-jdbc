package net.snowflake.client.internal.core;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemSetEnv;
import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemUnsetEnv;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.internal.jdbc.SnowflakeConnectString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class SFSessionPropertyTest {
  private static final String SF_ENABLE_WIF_AWS_EXTERNAL_ID = "SF_ENABLE_WIF_AWS_EXTERNAL_ID";
  private String originalEnvValue;

  @BeforeEach
  public void setUp() {
    originalEnvValue = System.getenv(SF_ENABLE_WIF_AWS_EXTERNAL_ID);
    systemUnsetEnv(SF_ENABLE_WIF_AWS_EXTERNAL_ID);
  }

  @AfterEach
  public void tearDown() {
    if (originalEnvValue != null) {
      systemSetEnv(SF_ENABLE_WIF_AWS_EXTERNAL_ID, originalEnvValue);
    } else {
      systemUnsetEnv(SF_ENABLE_WIF_AWS_EXTERNAL_ID);
    }
  }

  @Test
  public void testCheckApplicationName() throws SFException {
    String[] validApplicationName = {"test1234", "test_1234", "test-1234", "test.1234"};

    String[] invalidApplicationName = {"1234test", "test$A", "test<script>"};

    for (String valid : validApplicationName) {
      Object value = SFSessionProperty.checkPropertyValue(SFSessionProperty.APPLICATION, valid);

      assertThat((String) value, is(valid));
    }

    for (String invalid : invalidApplicationName) {
      SFException e =
          assertThrows(
              SFException.class,
              () -> {
                SFSessionProperty.checkPropertyValue(SFSessionProperty.APPLICATION, invalid);
              });
      assertThat(e.getVendorCode(), is(ErrorCode.INVALID_PARAMETER_VALUE.getMessageCode()));
    }
  }

  /**
   * The strict per-label allow-list that guards the account-&gt;host synthesis site
   * (SFConnectionConfigParser.createUrl) must NOT be applied here. checkPropertyValue runs on every
   * connection path, and on the ordinary URL path ACCOUNT is auto-derived from the host's first
   * label (SnowflakeConnectString:130-138) rather than supplied by the user. Every value that
   * derivation can produce has to keep passing.
   */
  @ParameterizedTest
  @CsvSource({
    // ordinary forms
    "jdbc:snowflake://abc.us-east-1.snowflakecomputing.com, abc",
    "jdbc:snowflake://abc_test.us-east-1.snowflakecomputing.com, abc_test",
    "jdbc:snowflake://a--b.us-east-1.snowflakecomputing.com, a--b",
    "jdbc:snowflake://myorg-myaccount.snowflakecomputing.com, myorg-myaccount",
    // IP-address host: the first label is a bare number
    "jdbc:snowflake://192.168.1.10:8080, 192",
    // .global. URL: the account is truncated at the last '-'
    "jdbc:snowflake://myacct-1234567.global.snowflakecomputing.com, myacct",
    // internal/regression host
    "jdbc:snowflake://snowflake.reg.local:8082, snowflake",
    // userinfo in the authority survives into the derived account
    "jdbc:snowflake://user@myacct.snowflakecomputing.com, user@myacct",
    // percent-escapes are preserved: getRawAuthority() is not decoded
    "jdbc:snowflake://my%5Facct.snowflakecomputing.com, my%5Facct",
    // registry-based authority characters that URI permits
    "jdbc:snowflake://a+b.snowflakecomputing.com, a+b",
    "jdbc:snowflake://a=b.snowflakecomputing.com, a=b",
    "jdbc:snowflake://a~b.snowflakecomputing.com, a~b",
    "jdbc:snowflake://a$b.snowflakecomputing.com, a$b",
    "jdbc:snowflake://a!b.snowflakecomputing.com, a!b",
    "jdbc:snowflake://a*b.snowflakecomputing.com, a*b",
    "jdbc:snowflake://a(b).snowflakecomputing.com, a(b)",
    "jdbc:snowflake://a;b.snowflakecomputing.com, a;b",
  })
  public void testAutoDerivedAccountFromUrlHostAlwaysPassesValidation(
      String url, String expectedAccount) throws SFException {
    SnowflakeConnectString cs = SnowflakeConnectString.parse(url, new Properties());
    assertTrue(cs.isValid(), "connect string did not parse: " + url);
    Object derived =
        cs.getParameters().get(SFSessionProperty.ACCOUNT.getPropertyKey().toUpperCase());
    assertEquals(expectedAccount, derived, "auto-derivation changed for " + url);

    // must not throw: this value was produced by the driver, not by the user
    assertEquals(derived, SFSessionProperty.checkPropertyValue(SFSessionProperty.ACCOUNT, derived));
  }

  /**
   * Defense in depth for the property path: an account value carrying a URL-authority delimiter is
   * rejected. None of these can be produced by the auto-derivation above.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "other.example.com?x",
        "other.example.com#x",
        "other.example.com:8080",
        "other.example.com/x",
        "other.example.com\\x",
        "other example com",
        "acct\t",
        "[::1]",
      })
  public void testAccountWithUrlAuthorityDelimiterIsRejected(String account) {
    SFException e =
        assertThrows(
            SFException.class,
            () -> SFSessionProperty.checkPropertyValue(SFSessionProperty.ACCOUNT, account));
    assertThat(e.getVendorCode(), is(ErrorCode.INVALID_PARAMETER_VALUE.getMessageCode()));
  }

  @Test
  public void testNullAndEmptyAccountAreLeftToTheirExistingHandling() throws SFException {
    assertEquals(null, SFSessionProperty.checkPropertyValue(SFSessionProperty.ACCOUNT, null));
    assertEquals("", SFSessionProperty.checkPropertyValue(SFSessionProperty.ACCOUNT, ""));
  }

  @Test
  public void testCustomSuffixForUserAgentHeaders() {
    String customSuffix = "test-suffix";
    String userAgentHeader = HttpUtil.buildUserAgent(customSuffix);

    assertThat(
        "user-agent header should contain the suffix ", userAgentHeader, endsWith(customSuffix));
  }

  @Test
  public void testInvalidMaxRetries() {
    SFException e =
        assertThrows(
            SFException.class,
            () -> {
              SFSessionProperty.checkPropertyValue(
                  SFSessionProperty.MAX_HTTP_RETRIES, "invalidValue");
            });
    assertThat(e.getVendorCode(), is(ErrorCode.INVALID_PARAMETER_VALUE.getMessageCode()));
  }

  @Test
  public void testvalidMaxRetries() throws SFException {
    int expectedVal = 10;
    Object value =
        SFSessionProperty.checkPropertyValue(SFSessionProperty.MAX_HTTP_RETRIES, expectedVal);

    assertThat("Integer value should match", (int) value == expectedVal);
  }

  @Test
  public void testInvalidPutGetMaxRetries() {
    SFException e =
        assertThrows(
            SFException.class,
            () -> {
              SFSessionProperty.checkPropertyValue(
                  SFSessionProperty.PUT_GET_MAX_RETRIES, "invalidValue");
            });
    assertThat(e.getVendorCode(), is(ErrorCode.INVALID_PARAMETER_VALUE.getMessageCode()));
  }

  @Test
  public void testvalidPutGetMaxRetries() throws SFException {
    int expectedVal = 10;
    Object value =
        SFSessionProperty.checkPropertyValue(SFSessionProperty.PUT_GET_MAX_RETRIES, expectedVal);

    assertThat("Integer value should match", (int) value == expectedVal);
  }

  @Test
  public void testEnableCopyResultSetPropertyRegistered() {
    SFSessionProperty prop = SFSessionProperty.lookupByKey("enableCopyResultSet");
    assertNotNull(prop);
    assertEquals(SFSessionProperty.ENABLE_COPY_RESULT_SET, prop);
    assertEquals(Boolean.class, prop.getValueType());
  }

  @Test
  void testEnableCopyResultSetDefaultFalse() {
    SFBaseSession session = mock(SFBaseSession.class, CALLS_REAL_METHODS);
    assertFalse(
        session.isEnableCopyResultSet(), "default must be false for backwards compatibility");
  }

  @Test
  void testEnableCopyResultSetCanBeSetTrue() {
    SFBaseSession session = mock(SFBaseSession.class, CALLS_REAL_METHODS);
    session.setEnableCopyResultSet(true);
    assertTrue(session.isEnableCopyResultSet());
  }

  @Test
  void testEnableCopyResultSetCanBeReset() {
    SFBaseSession session = mock(SFBaseSession.class, CALLS_REAL_METHODS);
    session.setEnableCopyResultSet(true);
    session.setEnableCopyResultSet(false);
    assertFalse(session.isEnableCopyResultSet(), "flag must be resettable to false");
  }

  @Test
  void testAddSFSessionPropertyWiresEnableCopyResultSet()
      throws SFException, ReflectiveOperationException {
    SFSession session = mock(SFSession.class, CALLS_REAL_METHODS);
    Field mapField = SFBaseSession.class.getDeclaredField("connectionPropertiesMap");
    mapField.setAccessible(true);
    mapField.set(session, new HashMap<>());
    session.addSFSessionProperty(
        SFSessionProperty.ENABLE_COPY_RESULT_SET.getPropertyKey(), Boolean.TRUE);
    assertTrue(session.isEnableCopyResultSet());
  }

  @Test
  public void testWorkloadIdentityAwsUseOutboundTokenPropertyRegistered() {
    SFSessionProperty prop = SFSessionProperty.lookupByKey("workloadIdentityAwsUseOutboundToken");
    assertNotNull(prop);
    assertEquals(SFSessionProperty.WORKLOAD_IDENTITY_AWS_USE_OUTBOUND_TOKEN, prop);
    assertEquals(Boolean.class, prop.getValueType());
  }

  @Test
  public void testWorkloadIdentityAwsUseOutboundTokenBooleanCoercion() throws SFException {
    Object value =
        SFSessionProperty.checkPropertyValue(
            SFSessionProperty.WORKLOAD_IDENTITY_AWS_USE_OUTBOUND_TOKEN, "true");
    assertEquals(Boolean.TRUE, value);

    Object valueFalse =
        SFSessionProperty.checkPropertyValue(
            SFSessionProperty.WORKLOAD_IDENTITY_AWS_USE_OUTBOUND_TOKEN, "false");
    assertEquals(Boolean.FALSE, valueFalse);
  }

  @Test
  public void shouldThrowWhenAwsExternalIdSetAndFeatureDisabled() {
    // SF_ENABLE_WIF_AWS_EXTERNAL_ID env var is not set in unit tests, so defaults to false
    Map<SFSessionProperty, Object> props = new HashMap<>();
    props.put(SFSessionProperty.WORKLOAD_IDENTITY_AWS_EXTERNAL_ID, "my-external-id");
    props.put(SFSessionProperty.AUTHENTICATOR, "workload_identity");
    props.put(SFSessionProperty.WORKLOAD_IDENTITY_PROVIDER, "aws");

    SFException e =
        assertThrows(SFException.class, () -> SFSession.checkAwsExternalIdEnabled(props));
    assertThat(e.getVendorCode(), is(ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR.getMessageCode()));
  }

  @Test
  public void shouldNotThrowWhenAuthenticatorIsNotWorkloadIdentity() throws SFException {
    Map<SFSessionProperty, Object> props = new HashMap<>();
    props.put(SFSessionProperty.AUTHENTICATOR, "snowflake");
    props.put(SFSessionProperty.WORKLOAD_IDENTITY_AWS_EXTERNAL_ID, "my-external-id");

    // Should not throw — external ID check only applies to WORKLOAD_IDENTITY + AWS
    SFSession.checkAwsExternalIdEnabled(props);
  }

  @Test
  public void shouldNotThrowWhenAwsExternalIdIsNullAndFeatureDisabled() throws SFException {
    Map<SFSessionProperty, Object> props = new HashMap<>();
    props.put(SFSessionProperty.WORKLOAD_IDENTITY_AWS_EXTERNAL_ID, null);

    // Should not throw — null means the property was not provided
    SFSession.checkAwsExternalIdEnabled(props);
  }

  @Test
  public void shouldNotThrowWhenAwsExternalIdIsEmptyAndFeatureDisabled() throws SFException {
    Map<SFSessionProperty, Object> props = new HashMap<>();
    props.put(SFSessionProperty.WORKLOAD_IDENTITY_AWS_EXTERNAL_ID, "");

    // Should not throw — empty string is treated the same as not provided
    SFSession.checkAwsExternalIdEnabled(props);
  }
}
