package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.HttpsURLConnection;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.DontRunOnGCP;
import net.snowflake.client.DontRunOnGithubActions;
import net.snowflake.client.core.SecurityUtil;
import org.apache.commons.codec.binary.Base64;
import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.crypto.fips.FipsStatus;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@Tag("fips")
public class ConnectionFipsIT extends AbstractDriverIT {
  private static final String JCE_PROVIDER_BOUNCY_CASTLE_FIPS = "BCFIPS";
  private static final String JCE_PROVIDER_SUN_JCE = "SunJCE";
  private static final String JCE_PROVIDER_SUN_RSA_SIGN = "SunRsaSign";
  private static final String JCE_KEYSTORE_BOUNCY_CASTLE = "BCFKS";
  private static final String JCE_KEYSTORE_JKS = "JKS";
  private static final String BOUNCY_CASTLE_RNG_HYBRID_MODE = "C:HYBRID;ENABLE{All};";

  private static final String SSL_ENABLED_PROTOCOLS = "TLSv1.2,TLSv1.1,TLSv1";
  private static final String SSL_ENABLED_CIPHERSUITES =
      "TLS_AES_128_GCM_SHA256,"
          + "TLS_AES_256_GCM_SHA384,"
          + "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,"
          + "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,"
          + "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,"
          + "TLS_RSA_WITH_AES_256_GCM_SHA384,"
          + "TLS_ECDH_ECDSA_WITH_AES_256_GCM_SHA384,"
          + "TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384,"
          + "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384,"
          + "TLS_DHE_DSS_WITH_AES_256_GCM_SHA384(,"
          + "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,"
          + "TLS_RSA_WITH_AES_128_GCM_SHA256,"
          + "TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256,"
          + "TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256,"
          + "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256,"
          + "TLS_DHE_DSS_WITH_AES_128_GCM_SHA256,"
          + "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384,"
          + "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,"
          + "TLS_RSA_WITH_AES_256_CBC_SHA256,"
          + "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384,"
          + "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384,"
          + "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256,"
          + "TLS_DHE_DSS_WITH_AES_256_CBC_SHA256,"
          + "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,"
          + "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,"
          + "TLS_RSA_WITH_AES_256_CBC_SHA,"
          + "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA,"
          + "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA,"
          + "TLS_DHE_RSA_WITH_AES_256_CBC_SHA,"
          + "TLS_DHE_DSS_WITH_AES_256_CBC_SHA,"
          + "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,"
          + "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,"
          + "TLS_RSA_WITH_AES_128_CBC_SHA256,"
          + "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256,"
          + "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256,"
          + "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256,"
          + "TLS_DHE_DSS_WITH_AES_128_CBC_SHA256,"
          + "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,"
          + "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,"
          + "TLS_RSA_WITH_AES_128_CBC_SHA,"
          + "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA,"
          + "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA,"
          + "TLS_DHE_RSA_WITH_AES_128_CBC_SHA,"
          + "TLS_DHE_DSS_WITH_AES_128_CBC_SHA";

  private static final String JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE = "javax.net.ssl.keyStoreType";
  private static final String JAVA_SYSTEM_PROPERTY_SSL_TRUSTSTORE_TYPE =
      "javax.net.ssl.trustStoreType";
  private static final String JAVA_SYSTEM_PROPERTY_SSL_PROTOCOLS = "jdk.tls.client.protocols";
  private static final String JAVA_SYSTEM_PROPERTY_SSL_CIPHERSUITES = "jdk.tls.client.cipherSuites";
  private static final String JAVA_SYSTEM_PROPERTY_SSL_NAMEDGROUPS = "jdk.tls.namedGroups";

  private static String JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE_ORIGINAL_VALUE;
  private static String JAVA_SYSTEM_PROPERTY_SSL_TRUSTSTORE_TYPE_ORIGINAL_VALUE;
  private static String JAVA_SYSTEM_PROPERTY_SSL_PROTOCOLS_ORIGINAL_VALUE;
  private static String JAVA_SYSTEM_PROPERTY_SSL_CIPHERSUITES_ORIGINAL_VALUE;

  private static Provider JCE_PROVIDER_SUN_JCE_PROVIDER_VALUE;
  private static Provider JCE_PROVIDER_SUN_RSA_SIGN_PROVIDER_VALUE;
  private static int JCE_PROVIDER_SUN_JCE_PROVIDER_POSITION;
  private static int JCE_PROVIDER_SUN_RSA_SIGN_PROVIDER_POSITION;

  @BeforeAll
  public static void setup() throws Exception {
    System.setProperty("javax.net.debug", "ssl");
    // Setting up the named group to avoid test failure on GCP environment.
    System.setProperty(JAVA_SYSTEM_PROPERTY_SSL_NAMEDGROUPS, "secp256r1, secp384r1, ffdhe2048, ffdhe3072");
    // get keystore types for BouncyCastle libraries
    JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE_ORIGINAL_VALUE =
        System.getProperty(JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE);
    JAVA_SYSTEM_PROPERTY_SSL_TRUSTSTORE_TYPE_ORIGINAL_VALUE =
        System.getProperty(JAVA_SYSTEM_PROPERTY_SSL_TRUSTSTORE_TYPE);

    // set keystore types for BouncyCastle libraries
    System.setProperty(JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE, JCE_KEYSTORE_BOUNCY_CASTLE);
    System.setProperty(JAVA_SYSTEM_PROPERTY_SSL_TRUSTSTORE_TYPE, JCE_KEYSTORE_JKS);
    // remove Java's standard encryption and SSL providers
    List<Provider> providers = Arrays.asList(Security.getProviders());
    JCE_PROVIDER_SUN_JCE_PROVIDER_VALUE = Security.getProvider(JCE_PROVIDER_SUN_JCE);
    JCE_PROVIDER_SUN_JCE_PROVIDER_POSITION = providers.indexOf(JCE_PROVIDER_SUN_JCE_PROVIDER_VALUE);
    JCE_PROVIDER_SUN_RSA_SIGN_PROVIDER_VALUE = Security.getProvider(JCE_PROVIDER_SUN_RSA_SIGN);
    JCE_PROVIDER_SUN_RSA_SIGN_PROVIDER_POSITION =
        providers.indexOf(JCE_PROVIDER_SUN_RSA_SIGN_PROVIDER_VALUE);
    Security.removeProvider(JCE_PROVIDER_SUN_JCE);
    Security.removeProvider(JCE_PROVIDER_SUN_RSA_SIGN);

    // workaround to connect to accounts.google.com over HTTPS, which consists
    // of disabling TLS 1.3 and disabling default SSL cipher suites that are
    // using CHACHA20_POLY1305 algorithms
    JAVA_SYSTEM_PROPERTY_SSL_PROTOCOLS_ORIGINAL_VALUE =
        System.getProperty(JAVA_SYSTEM_PROPERTY_SSL_PROTOCOLS);
    JAVA_SYSTEM_PROPERTY_SSL_CIPHERSUITES_ORIGINAL_VALUE =
        System.getProperty(JAVA_SYSTEM_PROPERTY_SSL_CIPHERSUITES);
    System.setProperty(JAVA_SYSTEM_PROPERTY_SSL_PROTOCOLS, SSL_ENABLED_PROTOCOLS);
    System.setProperty(JAVA_SYSTEM_PROPERTY_SSL_CIPHERSUITES, SSL_ENABLED_CIPHERSUITES);
    /*
     * Insert BouncyCastle's FIPS-compliant encryption and SSL providers.
     */
    BouncyCastleFipsProvider bcFipsProvider =
        new BouncyCastleFipsProvider(BOUNCY_CASTLE_RNG_HYBRID_MODE);

    /*
     * We remove BCFIPS provider pessimistically. This is a no-op if provider
     * does not exist. This is necessary to always add it to the first
     * position when calling insertProviderAt.
     *
     * JavaDoc for insertProviderAt states:
     *   "A provider cannot be added if it is already installed."
     */
    Security.removeProvider(JCE_PROVIDER_BOUNCY_CASTLE_FIPS);
    Security.insertProviderAt(bcFipsProvider, 1);
    if (!CryptoServicesRegistrar.isInApprovedOnlyMode()) {
      if (FipsStatus.isReady()) {
        CryptoServicesRegistrar.setApprovedOnlyMode(true);
      } else {
        throw new RuntimeException(
            "FIPS is not ready to be enabled and FIPS " + "mode is required for this test to run");
      }
    }

    // attempts an SSL connection to Google
    // connectToGoogle();
  }

  @AfterAll
  public static void teardown() throws Exception {
    // Remove BouncyCastle FIPS Provider
    Security.removeProvider(JCE_PROVIDER_BOUNCY_CASTLE_FIPS);

    // Restore ciphers removed to connect to accounts.google.com
    if (JAVA_SYSTEM_PROPERTY_SSL_PROTOCOLS_ORIGINAL_VALUE == null) {
      System.clearProperty(JAVA_SYSTEM_PROPERTY_SSL_PROTOCOLS);
    } else {
      System.setProperty(
          JAVA_SYSTEM_PROPERTY_SSL_PROTOCOLS, JAVA_SYSTEM_PROPERTY_SSL_PROTOCOLS_ORIGINAL_VALUE);
    }
    if (JAVA_SYSTEM_PROPERTY_SSL_CIPHERSUITES_ORIGINAL_VALUE == null) {
      System.clearProperty(JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE);
    } else {
      System.setProperty(
          JAVA_SYSTEM_PROPERTY_SSL_CIPHERSUITES,
          JAVA_SYSTEM_PROPERTY_SSL_CIPHERSUITES_ORIGINAL_VALUE);
    }

    // remove Java's standard encryption and SSL providers
    Security.insertProviderAt(
        JCE_PROVIDER_SUN_JCE_PROVIDER_VALUE, JCE_PROVIDER_SUN_JCE_PROVIDER_POSITION);
    Security.insertProviderAt(
        JCE_PROVIDER_SUN_RSA_SIGN_PROVIDER_VALUE, JCE_PROVIDER_SUN_RSA_SIGN_PROVIDER_POSITION);

    // Restore previous keystore values
    if (JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE_ORIGINAL_VALUE == null) {
      System.clearProperty(JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE);
    } else {
      System.setProperty(
          JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE,
          JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE_ORIGINAL_VALUE);
    }
    if (JAVA_SYSTEM_PROPERTY_SSL_TRUSTSTORE_TYPE_ORIGINAL_VALUE == null) {
      System.clearProperty(JAVA_SYSTEM_PROPERTY_SSL_TRUSTSTORE_TYPE);
    } else {
      System.setProperty(
          JAVA_SYSTEM_PROPERTY_SSL_TRUSTSTORE_TYPE,
          JAVA_SYSTEM_PROPERTY_SSL_TRUSTSTORE_TYPE_ORIGINAL_VALUE);
    }
    System.clearProperty(SecurityUtil.ENABLE_BOUNCYCASTLE_PROVIDER_JVM);
    // clear the named group.
    System.clearProperty(JAVA_SYSTEM_PROPERTY_SSL_NAMEDGROUPS);
    // attempts an SSL connection to Google
    // connectToGoogle();
  }

  @Test
  public void connectWithFips() throws SQLException {
    Connection con = getConnection();
    Statement statement = con.createStatement();
    ResultSet resultSet = statement.executeQuery("show parameters");
    assertTrue(resultSet.next());
    assertFalse(con.isClosed());
    statement.close();
    con.close();
    assertTrue(con.isClosed());
    con.close(); // ensure no exception
  }

  @Test
  @DontRunOnGithubActions
  public void connectWithFipsKeyPair() throws Exception {
    Map<String, String> parameters = getConnectionParameters();
    String testUser = parameters.get("user");

    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("use role accountadmin");
    String pathfile = getFullPathFileInResource("rsa_key.pub");
    String pubKey = new String(Files.readAllBytes(Paths.get(pathfile)));
    pubKey = pubKey.replace("-----BEGIN PUBLIC KEY-----", "");
    pubKey = pubKey.replace("-----END PUBLIC KEY-----", "");
    statement.execute(String.format("alter user %s set rsa_public_key='%s'", testUser, pubKey));
    connection.close();

    // PKCS8 private key file. No PKCS1 is supported.
    String privateKeyLocation = getFullPathFileInResource("rsa_key.p8");
    String uri = parameters.get("uri") + "/?private_key_file=" + privateKeyLocation;
    Properties properties = new Properties();
    properties.put("account", parameters.get("account"));
    properties.put("user", testUser);
    properties.put("ssl", parameters.get("ssl"));
    properties.put("port", parameters.get("port"));
    connection = DriverManager.getConnection(uri, properties);
    assertNotNull(connection);
    connection.close();
  }

  @Test
  @DontRunOnGithubActions
  public void testConnectUsingKeyPair() throws Exception {
    Map<String, String> parameters = getConnectionParameters();
    String testUser = parameters.get("user");

    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA", "BCFIPS");
    SecureRandom random = SecureRandom.getInstance("DEFAULT", "BCFIPS");
    keyPairGenerator.initialize(2048, random);

    KeyPair keyPair = keyPairGenerator.generateKeyPair();
    PublicKey publicKey = keyPair.getPublic();
    PrivateKey privateKey = keyPair.getPrivate();

    try (Connection connection = getConnection()) {
      Statement statement = connection.createStatement();
      statement.execute("use role accountadmin");
      String encodePublicKey = Base64.encodeBase64String(publicKey.getEncoded());
      statement.execute(
          String.format("alter user %s set rsa_public_key='%s'", testUser, encodePublicKey));
    }

    String uri = parameters.get("uri");

    Properties properties = new Properties();
    properties.put("account", parameters.get("account"));
    properties.put("user", testUser);
    properties.put("ssl", parameters.get("ssl"));
    properties.put("port", parameters.get("port"));

    // test correct private key one
    properties.put("privateKey", privateKey);
    DriverManager.getConnection(uri, properties).close();
  }

  /**
   * Test case for connecting with FIPS and executing a query.
   */
  @Test
  public void connectWithFipsAndQuery() throws SQLException {
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      ResultSet resultSet =
          statement.executeQuery(
              "select seq8(), randstr(100, random()) from table(generator(rowcount=>10000))");
      int cnt = 0;
      while (resultSet.next()) {
        assertNotNull(resultSet.getInt(1));
        assertNotNull(resultSet.getString(2));
        cnt++;
      }
      assertEquals(cnt, 10000);
    }
  }

  @Test
  public void connectWithFipsAndPut() throws Exception {
    try (Connection con = getConnection()) {
      // put files
      ResultSet resultSet =
          con.createStatement()
              .executeQuery("PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @~");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      assertEquals(cnt, 1);
    }
  }

  /** Added in > 3.15.1 */
  @Test
  @DontRunOnGithubActions
  public void connectWithFipsKeyPairWithBouncyCastle() throws Exception {
    System.setProperty(SecurityUtil.ENABLE_BOUNCYCASTLE_PROVIDER_JVM, "true");
    connectWithFipsKeyPair();
  }

  /** Added in > 3.15.1 */
  @Test
  @DontRunOnGithubActions
  public void testConnectUsingKeyPairWithBouncyCastle() throws Exception {
    System.setProperty(SecurityUtil.ENABLE_BOUNCYCASTLE_PROVIDER_JVM, "true");
    testConnectUsingKeyPair();
  }

  private static void connectToGoogle() throws Exception {
    URL url = new URL("https://www.google.com/");
    HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
    int code = con.getResponseCode();
    if (code != 200) {
      throw new Exception("Got " + code + " instead of HTTP_OK");
    }

    System.out.println("Connected to Google successfully");
  }
}
