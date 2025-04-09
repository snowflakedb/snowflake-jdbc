package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import com.amazonaws.Protocol;
import com.amazonaws.http.apache.SdkProxyRoutePlanner;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import net.snowflake.client.jdbc.OCSPErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.DecorrelatedJitterBackoff;
import net.snowflake.client.util.SFPair;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLInitializationException;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1Integer;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.ASN1OctetString;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.DLSequence;
import org.bouncycastle.asn1.ocsp.CertID;
import org.bouncycastle.asn1.oiw.OIWObjectIdentifiers;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.Certificate;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.TBSCertificate;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.ocsp.BasicOCSPResp;
import org.bouncycastle.cert.ocsp.CertificateID;
import org.bouncycastle.cert.ocsp.CertificateStatus;
import org.bouncycastle.cert.ocsp.OCSPException;
import org.bouncycastle.cert.ocsp.OCSPReq;
import org.bouncycastle.cert.ocsp.OCSPReqBuilder;
import org.bouncycastle.cert.ocsp.OCSPResp;
import org.bouncycastle.cert.ocsp.RevokedStatus;
import org.bouncycastle.cert.ocsp.SingleResp;
import org.bouncycastle.operator.DigestCalculator;

/**
 * SFTrustManager is a composite of TrustManager of the default JVM TrustManager and Snowflake OCSP
 * revocation status checker. Use this when initializing SSLContext object.
 *
 * <pre>{@code
 * TrustManager[] trustManagers = {new SFTrustManager()};
 * SSLContext sslContext = SSLContext.getInstance("TLS");
 * sslContext.init(null, trustManagers, null);
 * }</pre>
 */
public class SFTrustManager extends X509ExtendedTrustManager {
  /** Test System Parameters. Not used in the production */
  public static final String SF_OCSP_RESPONSE_CACHE_SERVER_URL =
      "SF_OCSP_RESPONSE_CACHE_SERVER_URL";

  public static final String SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED =
      "SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED";
  public static final String SF_OCSP_TEST_INJECT_VALIDITY_ERROR =
      "SF_OCSP_TEST_INJECT_VALIDITY_ERROR";
  public static final String SF_OCSP_TEST_INJECT_UNKNOWN_STATUS =
      "SF_OCSP_TEST_INJECT_UNKNOWN_STATUS";
  public static final String SF_OCSP_TEST_RESPONDER_URL = "SF_OCSP_TEST_RESPONDER_URL";
  public static final String SF_OCSP_TEST_OCSP_RESPONSE_CACHE_SERVER_TIMEOUT =
      "SF_OCSP_TEST_OCSP_RESPONSE_CACHE_SERVER_TIMEOUT";
  public static final String SF_OCSP_TEST_OCSP_RESPONDER_TIMEOUT =
      "SF_OCSP_TEST_OCSP_RESPONDER_TIMEOUT";
  public static final String SF_OCSP_TEST_INVALID_SIGNING_CERT =
      "SF_OCSP_TEST_INVALID_SIGNING_CERT";
  public static final String SF_OCSP_TEST_NO_OCSP_RESPONDER_URL =
      "SF_OCSP_TEST_NO_OCSP_RESPONDER_URL";
  /** OCSP response cache file name. Should be identical to other driver's cache file name. */
  static final String CACHE_FILE_NAME = "ocsp_response_cache.json";

  private static final SFLogger logger = SFLoggerFactory.getLogger(SFTrustManager.class);
  private static final ASN1ObjectIdentifier OIDocsp =
      new ASN1ObjectIdentifier("1.3.6.1.5.5.7.48.1").intern();
  private static final ASN1ObjectIdentifier SHA1RSA =
      new ASN1ObjectIdentifier("1.2.840.113549.1.1.5").intern();
  private static final ASN1ObjectIdentifier SHA256RSA =
      new ASN1ObjectIdentifier("1.2.840.113549.1.1.11").intern();
  private static final ASN1ObjectIdentifier SHA384RSA =
      new ASN1ObjectIdentifier("1.2.840.113549.1.1.12").intern();
  private static final ASN1ObjectIdentifier SHA512RSA =
      new ASN1ObjectIdentifier("1.2.840.113549.1.1.13").intern();

  private static final String ALGORITHM_SHA1_NAME = "SHA-1";
  /** Object mapper for JSON encoding and decoding */
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();
  /** System property name to specify cache directory. */
  private static final String CACHE_DIR_PROP = "net.snowflake.jdbc.ocspResponseCacheDir";
  /** Environment name to specify the cache directory. Used if system property not set. */
  private static final String CACHE_DIR_ENV = "SF_OCSP_RESPONSE_CACHE_DIR";
  /** OCSP response cache entry expiration time (s) */
  private static final long CACHE_EXPIRATION_IN_SECONDS = 432000L;
  /** OCSP response cache lock file expiration time (s) */
  private static final long CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS = 60L;
  /** Default OCSP Cache server connection timeout */
  private static final int DEFAULT_OCSP_CACHE_SERVER_CONNECTION_TIMEOUT = 5000;
  /** Default OCSP responder connection timeout */
  private static final int DEFAULT_OCSP_RESPONDER_CONNECTION_TIMEOUT = 10000;
  /** Default OCSP Cache server host name prefix */
  private static final String DEFAULT_OCSP_CACHE_HOST_PREFIX = "http://ocsp.snowflakecomputing.";
  /** Default domain for OCSP cache host */
  private static final String DEFAULT_OCSP_CACHE_HOST_DOMAIN = "com";

  /** OCSP response file cache directory */
  private static final FileCacheManager fileCacheManager;
  /** Tolerable validity date range ratio. */
  private static final float TOLERABLE_VALIDITY_RANGE_RATIO = 0.01f;
  /** Maximum clocktime skew (ms) */
  private static final long MAX_CLOCK_SKEW_IN_MILLISECONDS = 900000L;
  /** Minimum cache warm up time (ms) */
  private static final long MIN_CACHE_WARMUP_TIME_IN_MILLISECONDS = 18000000L;
  /** Initial sleeping time in retry (ms) */
  private static final long INITIAL_SLEEPING_TIME_IN_MILLISECONDS = 1000L;
  /** Maximum sleeping time in retry (ms) */
  private static final long MAX_SLEEPING_TIME_IN_MILLISECONDS = 16000L;
  /** Map from signature algorithm ASN1 object to the name. */
  private static final Map<ASN1ObjectIdentifier, String> SIGNATURE_OID_TO_STRING =
      new ConcurrentHashMap<>();
  /** Map from OCSP response code to a string representation. */
  private static final Map<Integer, String> OCSP_RESPONSE_CODE_TO_STRING =
      new ConcurrentHashMap<>();

  private static final Object ROOT_CA_LOCK = new Object();
  /** OCSP Response cache */
  private static final Map<OcspResponseCacheKey, SFPair<Long, String>> OCSP_RESPONSE_CACHE =
      new ConcurrentHashMap<>();
  /** Date and timestamp format */
  private static final SimpleDateFormat DATE_FORMAT_UTC =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  /** OCSP Response Cache server Retry URL pattern */
  static String SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN;
  /** OCSP response cache server URL. */
  static String SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE;

  private static JcaX509CertificateConverter CONVERTER_X509 = new JcaX509CertificateConverter();
  /** RootCA cache */
  private static Map<Integer, Certificate> ROOT_CA = new ConcurrentHashMap<>();

  private static final AtomicBoolean WAS_CACHE_UPDATED = new AtomicBoolean();
  private static final AtomicBoolean WAS_CACHE_READ = new AtomicBoolean();
  /** OCSP HTTP client */
  private static Map<Integer, CloseableHttpClient> ocspCacheServerClient =
      new ConcurrentHashMap<>();
  /** OCSP event types */
  public static String SF_OCSP_EVENT_TYPE_REVOKED_CERTIFICATE_ERROR = "RevokedCertificateError";

  public static String SF_OCSP_EVENT_TYPE_VALIDATION_ERROR = "OCSPValidationError";

  static {
    // init OCSP response cache file manager
    fileCacheManager =
        FileCacheManager.builder()
            .setCacheDirectorySystemProperty(CACHE_DIR_PROP)
            .setCacheDirectoryEnvironmentVariable(CACHE_DIR_ENV)
            .setBaseCacheFileName(CACHE_FILE_NAME)
            .setCacheFileLockExpirationInSeconds(CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS)
            .setOnlyOwnerPermissions(false)
            .build();
  }

  static {
    SIGNATURE_OID_TO_STRING.put(SHA1RSA, "SHA1withRSA");
    SIGNATURE_OID_TO_STRING.put(SHA256RSA, "SHA256withRSA");
    SIGNATURE_OID_TO_STRING.put(SHA384RSA, "SHA384withRSA");
    SIGNATURE_OID_TO_STRING.put(SHA512RSA, "SHA512withRSA");
  }

  static {
    OCSP_RESPONSE_CODE_TO_STRING.put(OCSPResp.SUCCESSFUL, "successful");
    OCSP_RESPONSE_CODE_TO_STRING.put(OCSPResp.MALFORMED_REQUEST, "malformedRequest");
    OCSP_RESPONSE_CODE_TO_STRING.put(OCSPResp.INTERNAL_ERROR, "internalError");
    OCSP_RESPONSE_CODE_TO_STRING.put(OCSPResp.TRY_LATER, "tryLater");
    OCSP_RESPONSE_CODE_TO_STRING.put(OCSPResp.SIG_REQUIRED, "sigRequired");
    OCSP_RESPONSE_CODE_TO_STRING.put(OCSPResp.UNAUTHORIZED, "unauthorized");
  }

  static {
    DATE_FORMAT_UTC.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  /** The default JVM Trust manager. */
  private final X509TrustManager trustManager;
  /** The default JVM Extended Trust Manager */
  private final X509ExtendedTrustManager exTrustManager;

  OCSPCacheServer ocspCacheServer = new OCSPCacheServer();
  /** OCSP mode */
  private OCSPMode ocspMode;

  private static HttpClientSettingsKey proxySettingsKey;

  /**
   * Constructor with the cache file. If not specified, the default cachefile is used.
   *
   * @param key HttpClientSettingsKey
   * @param cacheFile cache file.
   */
  SFTrustManager(HttpClientSettingsKey key, File cacheFile) {
    this.ocspMode = key.getOcspMode();
    this.proxySettingsKey = key;
    this.trustManager = getTrustManager(KeyManagerFactory.getDefaultAlgorithm());

    this.exTrustManager =
        (X509ExtendedTrustManager) getTrustManager(KeyManagerFactory.getDefaultAlgorithm());

    checkNewOCSPEndpointAvailability();

    if (cacheFile != null) {
      fileCacheManager.overrideCacheFile(cacheFile);
    }
    if (!WAS_CACHE_READ.getAndSet(true)) {
      // read cache file once
      JsonNode res = fileCacheManager.readCacheFile();
      readJsonStoreCache(res);
    }

    logger.debug(
        "Initializing trust manager with OCSP mode: {}, cache file: {}", ocspMode, cacheFile);
  }

  /** Deletes OCSP response cache file from disk. */
  public static void deleteCache() {
    fileCacheManager.deleteCacheFile();
  }

  public static void cleanTestSystemParameters() {
    System.clearProperty(SF_OCSP_RESPONSE_CACHE_SERVER_URL);
    System.clearProperty(SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED);
    System.clearProperty(SF_OCSP_TEST_INJECT_VALIDITY_ERROR);
    System.clearProperty(SF_OCSP_TEST_INJECT_UNKNOWN_STATUS);
    System.clearProperty(SF_OCSP_TEST_RESPONDER_URL);
    System.clearProperty(SF_OCSP_TEST_OCSP_RESPONDER_TIMEOUT);
    System.clearProperty(SF_OCSP_TEST_OCSP_RESPONSE_CACHE_SERVER_TIMEOUT);
    System.clearProperty(SF_OCSP_TEST_INVALID_SIGNING_CERT);
    System.clearProperty(SF_OCSP_TEST_NO_OCSP_RESPONDER_URL);
  }

  /**
   * Reset OCSP Cache server URL
   *
   * @param ocspCacheServerUrl OCSP Cache server URL
   */
  static void resetOCSPResponseCacherServerURL(String ocspCacheServerUrl) throws IOException {
    if (ocspCacheServerUrl == null || SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN != null) {
      return;
    }
    SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE = ocspCacheServerUrl;
    if (!SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE.startsWith(DEFAULT_OCSP_CACHE_HOST_PREFIX)) {
      URL url = new URL(SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE);
      if (url.getPort() > 0) {
        SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN =
            String.format(
                "%s://%s:%d/retry/%s", url.getProtocol(), url.getHost(), url.getPort(), "%s/%s");
      } else {
        SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN =
            String.format("%s://%s/retry/%s", url.getProtocol(), url.getHost(), "%s/%s");
      }
      logger.debug(
          "Reset OCSP response cache server URL to: {}",
          SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN);
    }
  }

  static void setOCSPResponseCacheServerURL(String serverURL) {
    String ocspCacheUrl = systemGetProperty(SF_OCSP_RESPONSE_CACHE_SERVER_URL);
    if (ocspCacheUrl != null) {
      SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE = ocspCacheUrl;
    }
    try {
      ocspCacheUrl = systemGetEnv(SF_OCSP_RESPONSE_CACHE_SERVER_URL);
      if (ocspCacheUrl != null) {
        SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE = ocspCacheUrl;
      }
    } catch (Throwable ex) {
      logger.debug(
          "Failed to get environment variable " + SF_OCSP_RESPONSE_CACHE_SERVER_URL + ". Ignored",
          true);
    }
    if (SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE == null) {
      String topLevelDomain = DEFAULT_OCSP_CACHE_HOST_DOMAIN;
      try {
        URL url = new URL(serverURL);
        int domainIndex = url.getHost().lastIndexOf(".") + 1;
        topLevelDomain = url.getHost().substring(domainIndex);
      } catch (Exception e) {
        logger.debug("Exception while setting top level domain (for OCSP)", e);
      }
      SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE =
          String.format("%s%s/%s", DEFAULT_OCSP_CACHE_HOST_PREFIX, topLevelDomain, CACHE_FILE_NAME);
    }
    logger.debug("Set OCSP response cache server to: {}", SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE);
  }

  private static boolean useOCSPResponseCacheServer() {
    String ocspCacheServerEnabled = systemGetProperty(SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED);
    if (Boolean.FALSE.toString().equalsIgnoreCase(ocspCacheServerEnabled)) {
      logger.debug("No OCSP Response Cache Server is used.", false);
      return false;
    }
    try {
      ocspCacheServerEnabled = systemGetEnv(SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED);
      if (Boolean.FALSE.toString().equalsIgnoreCase(ocspCacheServerEnabled)) {
        logger.debug("No OCSP Response Cache Server is used.", false);
        return false;
      }
    } catch (Throwable ex) {
      logger.debug(
          "Failed to get environment variable "
              + SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED
              + ". Ignored",
          false);
    }
    return true;
  }

  /**
   * Convert cache key to base64 encoded cert id
   *
   * @param ocsp_cache_key Cache key to encode
   */
  private static String encodeCacheKey(OcspResponseCacheKey ocsp_cache_key) {
    try {
      DigestCalculator digest = new SHA1DigestCalculator();
      AlgorithmIdentifier algo = digest.getAlgorithmIdentifier();
      ASN1OctetString nameHash = ASN1OctetString.getInstance(ocsp_cache_key.nameHash);
      ASN1OctetString keyHash = ASN1OctetString.getInstance(ocsp_cache_key.keyHash);
      ASN1Integer snumber = new ASN1Integer(ocsp_cache_key.serialNumber);
      CertID cid = new CertID(algo, nameHash, keyHash, snumber);
      return Base64.encodeBase64String(cid.toASN1Primitive().getEncoded());
    } catch (Exception ex) {
      logger.debug("Failed to encode cache key to base64 encoded cert id", false);
    }
    return null;
  }

  /**
   * CertificateID to string
   *
   * @param certificateID CertificateID
   * @return a string representation of CertificateID
   */
  private static String CertificateIDToString(CertificateID certificateID) {
    return String.format(
        "CertID. NameHash: %s, KeyHash: %s, Serial Number: %s",
        HexUtil.byteToHexString(certificateID.getIssuerNameHash()),
        HexUtil.byteToHexString(certificateID.getIssuerKeyHash()),
        MessageFormat.format("{0,number,#}", certificateID.getSerialNumber()));
  }

  /**
   * Decodes OCSP Response Cache key from JSON
   *
   * @param elem A JSON element
   * @return OcspResponseCacheKey object
   */
  private static SFPair<OcspResponseCacheKey, SFPair<Long, String>> decodeCacheFromJSON(
      Map.Entry<String, JsonNode> elem) throws IOException {
    long currentTimeSecond = new Date().getTime() / 1000;
    byte[] certIdDer = Base64.decodeBase64(elem.getKey());
    DLSequence rawCertId = (DLSequence) ASN1ObjectIdentifier.fromByteArray(certIdDer);
    ASN1Encodable[] rawCertIdArray = rawCertId.toArray();
    byte[] issuerNameHashDer = ((DEROctetString) rawCertIdArray[1]).getEncoded();
    byte[] issuerKeyHashDer = ((DEROctetString) rawCertIdArray[2]).getEncoded();
    BigInteger serialNumber = ((ASN1Integer) rawCertIdArray[3]).getValue();

    OcspResponseCacheKey k =
        new OcspResponseCacheKey(issuerNameHashDer, issuerKeyHashDer, serialNumber);

    JsonNode ocspRespBase64 = elem.getValue();
    if (!ocspRespBase64.isArray() || ocspRespBase64.size() != 2) {
      logger.debug("Invalid cache file format. Ignored", false);
      return null;
    }
    long producedAt = ocspRespBase64.get(0).asLong();
    String ocspResp = ocspRespBase64.get(1).asText();

    if (currentTimeSecond - CACHE_EXPIRATION_IN_SECONDS <= producedAt) {
      // add cache
      return SFPair.of(k, SFPair.of(producedAt, ocspResp));
    } else {
      // delete cache
      return SFPair.of(k, SFPair.of(producedAt, null));
    }
  }

  /**
   * Encode OCSP Response Cache to JSON
   *
   * @return JSON object
   */
  private static ObjectNode encodeCacheToJSON() {
    try {
      ObjectNode out = OBJECT_MAPPER.createObjectNode();
      for (Map.Entry<OcspResponseCacheKey, SFPair<Long, String>> elem :
          OCSP_RESPONSE_CACHE.entrySet()) {
        OcspResponseCacheKey key = elem.getKey();
        SFPair<Long, String> value0 = elem.getValue();
        long currentTimeSecond = value0.left;

        DigestCalculator digest = new SHA1DigestCalculator();
        AlgorithmIdentifier algo = digest.getAlgorithmIdentifier();
        ASN1OctetString nameHash = ASN1OctetString.getInstance(key.nameHash);
        ASN1OctetString keyHash = ASN1OctetString.getInstance(key.keyHash);
        ASN1Integer serialNumber = new ASN1Integer(key.serialNumber);
        CertID cid = new CertID(algo, nameHash, keyHash, serialNumber);
        ArrayNode vout = OBJECT_MAPPER.createArrayNode();
        vout.add(currentTimeSecond);
        vout.add(value0.right);
        out.set(Base64.encodeBase64String(cid.toASN1Primitive().getEncoded()), vout);
      }
      return out;
    } catch (IOException ex) {
      logger.debug("Failed to encode ASN1 object.", false);
    }
    return null;
  }

  private static synchronized void readJsonStoreCache(JsonNode m) {
    if (m == null || !m.getNodeType().equals(JsonNodeType.OBJECT)) {
      logger.debug("Invalid cache file format.", false);
      return;
    }
    try {
      for (Iterator<Map.Entry<String, JsonNode>> itr = m.fields(); itr.hasNext(); ) {
        SFPair<OcspResponseCacheKey, SFPair<Long, String>> ky = decodeCacheFromJSON(itr.next());
        if (ky != null && ky.right != null && ky.right.right != null) {
          // valid range. cache the result in memory
          OCSP_RESPONSE_CACHE.put(ky.left, ky.right);
          WAS_CACHE_UPDATED.set(true);
        } else if (ky != null && OCSP_RESPONSE_CACHE.containsKey(ky.left)) {
          // delete it from the cache if no OCSP response is back.
          OCSP_RESPONSE_CACHE.remove(ky.left);
          WAS_CACHE_UPDATED.set(true);
        }
      }
    } catch (IOException ex) {
      logger.debug("Failed to decode the cache file", false);
    }
  }

  /**
   * Verifies the signature of the data
   *
   * @param cert a certificate for public key.
   * @param sig signature in a byte array.
   * @param data data in a byte array.
   * @param idf algorithm identifier object.
   * @throws CertificateException raises if the verification fails.
   */
  private static void verifySignature(
      X509CertificateHolder cert, byte[] sig, byte[] data, AlgorithmIdentifier idf)
      throws CertificateException {
    try {
      String algorithm = SIGNATURE_OID_TO_STRING.get(idf.getAlgorithm());
      if (algorithm == null) {
        throw new NoSuchAlgorithmException(
            String.format("Unsupported signature OID. OID: %s", idf));
      }
      Signature signer = Signature.getInstance(algorithm);

      X509Certificate c = CONVERTER_X509.getCertificate(cert);
      signer.initVerify(c.getPublicKey());
      signer.update(data);
      if (!signer.verify(sig)) {
        throw new CertificateEncodingException(
            String.format(
                "Failed to verify the signature. Potentially the "
                    + "data was not generated by by the cert, %s",
                cert.getSubject()));
      }
    } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException ex) {
      throw new CertificateEncodingException("Failed to verify the signature.", ex);
    }
  }

  /**
   * Gets HttpClient object
   *
   * @return HttpClient
   */
  private static CloseableHttpClient getHttpClient(int timeout) {
    RequestConfig config =
        RequestConfig.custom()
            .setConnectTimeout(timeout)
            .setConnectionRequestTimeout(timeout)
            .setSocketTimeout(timeout)
            .build();

    Registry<ConnectionSocketFactory> registry =
        RegistryBuilder.<ConnectionSocketFactory>create()
            .register("http", new HttpUtil.SFConnectionSocketFactory())
            .build();

    // Build a connection manager with enough connections
    PoolingHttpClientConnectionManager connectionManager =
        new PoolingHttpClientConnectionManager(registry);
    connectionManager.setMaxTotal(1);
    connectionManager.setDefaultMaxPerRoute(10);

    HttpClientBuilder httpClientBuilder =
        HttpClientBuilder.create()
            .setDefaultRequestConfig(config)
            .setConnectionManager(connectionManager)
            // Support JVM proxy settings
            .useSystemProperties()
            .setRedirectStrategy(new DefaultRedirectStrategy())
            .disableCookieManagement();

    if (proxySettingsKey.usesProxy()) {
      // use the custom proxy properties
      HttpHost proxy =
          new HttpHost(proxySettingsKey.getProxyHost(), proxySettingsKey.getProxyPort());
      SdkProxyRoutePlanner sdkProxyRoutePlanner =
          new SdkProxyRoutePlanner(
              proxySettingsKey.getProxyHost(),
              proxySettingsKey.getProxyPort(),
              Protocol.HTTP,
              proxySettingsKey.getNonProxyHosts());
      httpClientBuilder = httpClientBuilder.setProxy(proxy).setRoutePlanner(sdkProxyRoutePlanner);
      if (!isNullOrEmpty(proxySettingsKey.getProxyUser())
          && !isNullOrEmpty(proxySettingsKey.getProxyPassword())) {
        Credentials credentials =
            new UsernamePasswordCredentials(
                proxySettingsKey.getProxyUser(), proxySettingsKey.getProxyPassword());
        AuthScope authScope =
            new AuthScope(proxySettingsKey.getProxyHost(), proxySettingsKey.getProxyPort());
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(authScope, credentials);
        httpClientBuilder = httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      }
    }
    // using the default HTTP client
    return httpClientBuilder.build();
  }

  private static long maxLong(long v1, long v2) {
    return Math.max(v1, v2);
  }

  /**
   * Calculates the tolerable validity time beyond the next update.
   *
   * <p>Sometimes CA's OCSP response update is delayed beyond the clock skew as the update is not
   * populated to all OCSP servers for certain period.
   *
   * @param thisUpdate the last update
   * @param nextUpdate the next update
   * @return the tolerable validity beyond the next update.
   */
  private static long calculateTolerableValidity(Date thisUpdate, Date nextUpdate) {
    return maxLong(
        (long)
            ((float) (nextUpdate.getTime() - thisUpdate.getTime())
                * TOLERABLE_VALIDITY_RANGE_RATIO),
        MIN_CACHE_WARMUP_TIME_IN_MILLISECONDS);
  }

  /**
   * Checks the validity
   *
   * @param currentTime the current time
   * @param thisUpdate the last update timestamp
   * @param nextUpdate the next update timestamp
   * @return true if valid or false
   */
  private static boolean isValidityRange(Date currentTime, Date thisUpdate, Date nextUpdate) {
    if (checkOCSPResponseValidityErrorParameter()) {
      return false; // test
    }
    long tolerableValidity = calculateTolerableValidity(thisUpdate, nextUpdate);
    return thisUpdate.getTime() - MAX_CLOCK_SKEW_IN_MILLISECONDS <= currentTime.getTime()
        && currentTime.getTime() <= nextUpdate.getTime() + tolerableValidity;
  }

  private static boolean checkOCSPResponseValidityErrorParameter() {
    String injectValidityError = systemGetProperty(SF_OCSP_TEST_INJECT_VALIDITY_ERROR);
    return Boolean.TRUE.toString().equalsIgnoreCase(injectValidityError);
  }

  /**
   * Is the test parameter enabled?
   *
   * @param key the test parameter
   * @return true if enabled otherwise false
   */
  private boolean isEnabledSystemTestParameter(String key) {
    return Boolean.TRUE.toString().equalsIgnoreCase(systemGetProperty(key));
  }

  /** fail open mode current state */
  private boolean isOCSPFailOpen() {
    return ocspMode == OCSPMode.FAIL_OPEN;
  }

  private void checkNewOCSPEndpointAvailability() {
    String new_ocsp_ept;
    try {
      new_ocsp_ept = systemGetEnv("SF_OCSP_ACTIVATE_NEW_ENDPOINT");
    } catch (Throwable ex) {
      logger.debug(
          "Could not get environment variable to check for New OCSP Endpoint Availability", false);
      new_ocsp_ept = systemGetProperty("net.snowflake.jdbc.ocsp_activate_new_endpoint");
    }
    ocspCacheServer.new_endpoint_enabled = new_ocsp_ept != null;
  }

  /**
   * Get TrustManager for the algorithm. This is mainly used to get the JVM default trust manager
   * and cache all of the root CA.
   *
   * @param algorithm algorithm.
   * @return TrustManager object.
   */
  private X509TrustManager getTrustManager(String algorithm) {
    try {
      TrustManagerFactory factory = TrustManagerFactory.getInstance(algorithm);
      factory.init((KeyStore) null);
      X509TrustManager ret = null;
      for (TrustManager tm : factory.getTrustManagers()) {
        // Multiple TrustManager may be attached. We just need X509 Trust
        // Manager here.
        if (tm instanceof X509TrustManager) {
          ret = (X509TrustManager) tm;
          break;
        }
      }
      if (ret == null) {
        return null;
      }
      synchronized (ROOT_CA_LOCK) {
        // cache root CA certificates for later use.
        if (ROOT_CA.isEmpty()) {
          for (X509Certificate cert : ret.getAcceptedIssuers()) {
            Certificate bcCert = Certificate.getInstance(cert.getEncoded());
            ROOT_CA.put(bcCert.getSubject().hashCode(), bcCert);
          }
        }
      }
      return ret;
    } catch (NoSuchAlgorithmException | KeyStoreException | CertificateEncodingException ex) {
      throw new SSLInitializationException(ex.getMessage(), ex);
    }
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    // default behavior
    trustManager.checkClientTrusted(chain, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    trustManager.checkServerTrusted(chain, authType);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, java.net.Socket socket)
      throws CertificateException {
    // default behavior
    this.exTrustManager.checkClientTrusted(chain, authType, socket);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine sslEngine)
      throws CertificateException {
    // default behavior
    exTrustManager.checkClientTrusted(chain, authType, sslEngine);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, java.net.Socket socket)
      throws CertificateException {
    // default behavior
    exTrustManager.checkServerTrusted(chain, authType, socket);
    String host = socket.getInetAddress().getHostName();
    this.validateRevocationStatus(chain, host);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine sslEngine)
      throws CertificateException {
    // default behavior
    exTrustManager.checkServerTrusted(chain, authType, sslEngine);
    this.validateRevocationStatus(chain, sslEngine.getPeerHost());
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return trustManager.getAcceptedIssuers();
  }

  /**
   * Certificate Revocation checks
   *
   * @param chain chain of certificates attached.
   * @param peerHost Hostname of the server
   * @throws CertificateException if any certificate validation fails
   */
  void validateRevocationStatus(X509Certificate[] chain, String peerHost)
      throws CertificateException {
    final List<Certificate> bcChain = convertToBouncyCastleCertificate(chain);
    final List<SFPair<Certificate, Certificate>> pairIssuerSubjectList =
        getPairIssuerSubject(bcChain);

    if (peerHost.startsWith("ocspssd")) {
      return;
    }

    if (ocspCacheServer.new_endpoint_enabled) {
      ocspCacheServer.resetOCSPResponseCacheServer(peerHost);
    }

    boolean isCached = isCached(pairIssuerSubjectList);
    if (useOCSPResponseCacheServer() && !isCached) {
      if (!ocspCacheServer.new_endpoint_enabled) {
        logger.debug(
            "Downloading OCSP response cache from the server. URL: {}",
            SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE);
      } else {
        logger.debug(
            "Downloading OCSP response cache from the server. URL: {}",
            ocspCacheServer.SF_OCSP_RESPONSE_CACHE_SERVER);
      }
      try {
        readOcspResponseCacheServer();
      } catch (SFOCSPException ex) {
        logger.debug(
            "Error downloading OCSP Response from cache server : {}."
                + "OCSP Responses will be fetched directly from the CA OCSP"
                + "Responder ",
            ex.getMessage());
      }
      // if the cache is downloaded from the server, it should be written
      // to the file cache at all times.
    }
    executeRevocationStatusChecks(pairIssuerSubjectList, peerHost);
    if (WAS_CACHE_UPDATED.getAndSet(false)) {
      JsonNode input = encodeCacheToJSON();
      fileCacheManager.writeCacheFile(input);
    }
  }

  /**
   * Executes the revocation status checks for all chained certificates
   *
   * @param pairIssuerSubjectList a list of pair of issuer and subject certificates.
   * @throws CertificateException raises if any error occurs.
   */
  private void executeRevocationStatusChecks(
      List<SFPair<Certificate, Certificate>> pairIssuerSubjectList, String peerHost)
      throws CertificateException {
    long currentTimeSecond = new Date().getTime() / 1000L;
    for (SFPair<Certificate, Certificate> pairIssuerSubject : pairIssuerSubjectList) {
      executeOneRevocationStatusCheck(pairIssuerSubject, currentTimeSecond, peerHost);
    }
  }

  private String generateFailOpenLog(String logData) {
    return "OCSP responder didn't respond correctly. Assuming certificate is "
        + "not revoked. Details: "
        + logData;
  }

  /**
   * Executes a single revocation status check
   *
   * @param pairIssuerSubject a pair of issuer and subject certificate
   * @param currentTimeSecond the current timestamp
   * @throws CertificateException if certificate exception is raised.
   */
  private void executeOneRevocationStatusCheck(
      SFPair<Certificate, Certificate> pairIssuerSubject, long currentTimeSecond, String peerHost)
      throws CertificateException {
    OCSPReq req;
    OcspResponseCacheKey keyOcspResponse;
    try {
      req = createRequest(pairIssuerSubject);
      CertID cid = req.getRequestList()[0].getCertID().toASN1Primitive();
      keyOcspResponse =
          new OcspResponseCacheKey(
              cid.getIssuerNameHash().getEncoded(),
              cid.getIssuerKeyHash().getEncoded(),
              cid.getSerialNumber().getValue());
    } catch (IOException ex) {
      throw new CertificateException(ex.getMessage(), ex);
    }

    long sleepTime = INITIAL_SLEEPING_TIME_IN_MILLISECONDS;
    DecorrelatedJitterBackoff backoff =
        new DecorrelatedJitterBackoff(sleepTime, MAX_SLEEPING_TIME_IN_MILLISECONDS);
    CertificateException error;
    boolean success = false;
    String ocspLog;
    OCSPTelemetryData telemetryData = new OCSPTelemetryData();
    telemetryData.setSfcPeerHost(peerHost);
    telemetryData.setCertId(encodeCacheKey(keyOcspResponse));
    telemetryData.setCacheEnabled(useOCSPResponseCacheServer());
    telemetryData.setOCSPMode(ocspMode);
    Throwable cause = null;
    try {
      final int maxRetryCounter = isOCSPFailOpen() ? 1 : 2;
      for (int retry = 0; retry < maxRetryCounter; ++retry) {
        try {
          SFPair<Long, String> value0 = OCSP_RESPONSE_CACHE.get(keyOcspResponse);
          OCSPResp ocspResp;
          try {
            try {
              if (value0 == null) {
                telemetryData.setCacheHit(false);
                ocspResp =
                    fetchOcspResponse(
                        pairIssuerSubject,
                        req,
                        encodeCacheKey(keyOcspResponse),
                        peerHost,
                        telemetryData);

                OCSP_RESPONSE_CACHE.put(
                    keyOcspResponse, SFPair.of(currentTimeSecond, ocspResponseToB64(ocspResp)));
                WAS_CACHE_UPDATED.set(true);
                value0 = SFPair.of(currentTimeSecond, ocspResponseToB64(ocspResp));
              } else {
                telemetryData.setCacheHit(true);
              }
            } catch (Throwable ex) {
              logger.debug(
                  "Exception occurred while trying to fetch OCSP Response - {}", ex.getMessage());
              throw new SFOCSPException(
                  OCSPErrorCode.OCSP_RESPONSE_FETCH_FAILURE,
                  "Exception occurred while trying to fetch OCSP Response",
                  ex);
            }

            logger.debug(
                "Validating. {}", CertificateIDToString(req.getRequestList()[0].getCertID()));
            try {
              validateRevocationStatusMain(pairIssuerSubject, value0.right);
              success = true;
              break;
            } catch (SFOCSPException ex) {
              if (ex.getErrorCode() != OCSPErrorCode.REVOCATION_CHECK_FAILURE) {
                throw ex;
              }
              throw new CertificateException(ex.getMessage(), ex);
            }
          } catch (SFOCSPException ex) {
            if (ex.getErrorCode() == OCSPErrorCode.CERTIFICATE_STATUS_REVOKED) {
              throw ex;
            } else {
              throw new CertificateException(ex.getMessage(), ex);
            }
          }
        } catch (CertificateException ex) {
          WAS_CACHE_UPDATED.set(OCSP_RESPONSE_CACHE.remove(keyOcspResponse) != null);
          if (WAS_CACHE_UPDATED.get()) {
            logger.debug("Deleting the invalid OCSP cache.", false);
          }

          cause = ex;
          logger.debug(
              "Retrying {}/{} after sleeping {} ms", retry + 1, maxRetryCounter, sleepTime);
          try {
            if (retry + 1 < maxRetryCounter) {
              Thread.sleep(sleepTime);
              sleepTime = backoff.nextSleepTime(sleepTime);
            }
          } catch (InterruptedException ex0) { // nop
          }
        }
      }
    } catch (SFOCSPException ex) {
      // Revoked Certificate
      error = new CertificateException(ex);
      ocspLog =
          telemetryData.generateTelemetry(SF_OCSP_EVENT_TYPE_REVOKED_CERTIFICATE_ERROR, error);
      logger.error(ocspLog, false);
      throw error;
    }

    if (!success) {
      if (cause != null) // cause is set in the above catch block
      {
        error =
            new CertificateException(
                "Certificate Revocation check failed. Could not retrieve OCSP Response.", cause);
        logger.debug(cause.getMessage(), false);
      } else {
        error =
            new CertificateException(
                "Certificate Revocation check failed. Could not retrieve OCSP Response.");
        logger.debug(error.getMessage(), false);
      }

      ocspLog = telemetryData.generateTelemetry(SF_OCSP_EVENT_TYPE_VALIDATION_ERROR, error);
      if (isOCSPFailOpen()) {
        // Log includes fail-open warning.
        logger.debug(generateFailOpenLog(ocspLog), false);
      } else {
        // still not success, raise an error.
        logger.debug(ocspLog, false);
        throw error;
      }
    }
  }

  /**
   * Is OCSP Response cached?
   *
   * @param pairIssuerSubjectList a list of pair of issuer and subject certificates
   * @return true if all of OCSP response are cached else false
   */
  private boolean isCached(List<SFPair<Certificate, Certificate>> pairIssuerSubjectList) {
    long currentTimeSecond = new Date().getTime() / 1000L;
    boolean isCached = true;
    try {
      for (SFPair<Certificate, Certificate> pairIssuerSubject : pairIssuerSubjectList) {
        OCSPReq req = createRequest(pairIssuerSubject);
        CertificateID certificateId = req.getRequestList()[0].getCertID();
        logger.debug(CertificateIDToString(certificateId), false);
        CertID cid = certificateId.toASN1Primitive();
        OcspResponseCacheKey k =
            new OcspResponseCacheKey(
                cid.getIssuerNameHash().getEncoded(),
                cid.getIssuerKeyHash().getEncoded(),
                cid.getSerialNumber().getValue());

        SFPair<Long, String> res = OCSP_RESPONSE_CACHE.get(k);
        if (res == null) {
          logger.debug("Not all OCSP responses for the certificate is in the cache.", false);
          isCached = false;
          break;
        } else if (currentTimeSecond - CACHE_EXPIRATION_IN_SECONDS > res.left) {
          logger.debug("Cache for CertID expired.", false);
          isCached = false;
          break;
        } else {
          try {
            validateRevocationStatusMain(pairIssuerSubject, res.right);
          } catch (SFOCSPException ex) {
            logger.debug(
                "Cache includes invalid OCSPResponse. "
                    + "Will download the OCSP cache from Snowflake OCSP server",
                false);
            isCached = false;
          }
        }
      }
    } catch (IOException ex) {
      logger.debug("Failed to encode CertID.", false);
    }
    return isCached;
  }

  /** Reads the OCSP response cache from the server. */
  private void readOcspResponseCacheServer() throws SFOCSPException {
    String ocspCacheServerInUse;

    if (ocspCacheServer.new_endpoint_enabled) {
      ocspCacheServerInUse = ocspCacheServer.SF_OCSP_RESPONSE_CACHE_SERVER;
    } else {
      ocspCacheServerInUse = SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE;
    }

    CloseableHttpResponse response = null;
    CloseableHttpClient httpClient =
        ocspCacheServerClient.computeIfAbsent(
            getOCSPCacheServerConnectionTimeout(),
            k -> getHttpClient(getOCSPCacheServerConnectionTimeout()));
    try {
      URI uri = new URI(ocspCacheServerInUse);
      HttpGet get = new HttpGet(uri);
      response = httpClient.execute(get);
      if (response == null || response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new IOException(
            String.format(
                "Failed to get the OCSP response from the OCSP " + "cache server: HTTP: %d",
                response != null ? response.getStatusLine().getStatusCode() : -1));
      }

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copy(response.getEntity().getContent(), out);
      JsonNode m = OBJECT_MAPPER.readTree(out.toByteArray());
      out.close();
      readJsonStoreCache(m);
      logger.debug("Successfully downloaded OCSP cache from the server.", false);
    } catch (IOException ex) {
      logger.debug(
          "Failed to read the OCSP response cache from the server. " + "Server: {}, Err: {}",
          ocspCacheServerInUse,
          ex);
    } catch (URISyntaxException ex) {
      logger.debug("Indicate that a string could not be parsed as a URI reference.", false);
      throw new SFOCSPException(
          OCSPErrorCode.INVALID_CACHE_SERVER_URL, "Invalid OCSP Cache Server URL used", ex);
    } finally {
      IOUtils.closeQuietly(response);
    }
  }

  private int getOCSPCacheServerConnectionTimeout() {
    int timeout = DEFAULT_OCSP_CACHE_SERVER_CONNECTION_TIMEOUT;
    if (systemGetProperty(SF_OCSP_TEST_OCSP_RESPONSE_CACHE_SERVER_TIMEOUT) != null) {
      try {
        timeout =
            Integer.parseInt(systemGetProperty(SF_OCSP_TEST_OCSP_RESPONSE_CACHE_SERVER_TIMEOUT));
      } catch (Exception ex) {
        // nop
      }
    }
    return timeout;
  }

  /**
   * Fetches OCSP response from OCSP server
   *
   * @param pairIssuerSubject a pair of issuer and subject certificates
   * @param req OCSP Request object
   * @return OCSP Response object
   * @throws CertificateEncodingException if any other error occurs
   */
  private OCSPResp fetchOcspResponse(
      SFPair<Certificate, Certificate> pairIssuerSubject,
      OCSPReq req,
      String cid_enc,
      String hname,
      OCSPTelemetryData telemetryData)
      throws CertificateEncodingException {
    CloseableHttpResponse response = null;
    try {
      byte[] ocspReqDer = req.getEncoded();
      String ocspReqDerBase64 = Base64.encodeBase64String(ocspReqDer);
      Set<String> ocspUrls = getOcspUrls(pairIssuerSubject.right);
      checkExistOCSPURL(ocspUrls);
      String ocspUrlStr = ocspUrls.iterator().next(); // first one
      ocspUrlStr = overrideOCSPURL(ocspUrlStr);
      telemetryData.setOcspUrl(ocspUrlStr);
      telemetryData.setOcspReq(ocspReqDerBase64);

      URL url;
      String path = "";
      if (!ocspCacheServer.new_endpoint_enabled) {
        String urlEncodedOCSPReq = URLUtil.urlEncode(ocspReqDerBase64);
        if (SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN != null) {
          URL ocspUrl = new URL(ocspUrlStr);
          if (!isNullOrEmpty(ocspUrl.getPath())) {
            path = ocspUrl.getPath();
          }
          if (ocspUrl.getPort() > 0) {
            url =
                new URL(
                    String.format(
                        SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN,
                        ocspUrl.getHost() + ":" + ocspUrl.getPort() + path,
                        urlEncodedOCSPReq));
          } else {
            url =
                new URL(
                    String.format(
                        SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN,
                        ocspUrl.getHost() + path,
                        urlEncodedOCSPReq));
          }

        } else {
          url = new URL(String.format("%s/%s", ocspUrlStr, urlEncodedOCSPReq));
        }
        logger.debug("Not hit cache. Fetching OCSP response from CA OCSP server. {}", url);
      } else {
        url = new URL(ocspCacheServer.SF_OCSP_RESPONSE_RETRY_URL);
        logger.debug(
            "Not hit cache. Fetching OCSP response from Snowflake OCSP Response Fetcher. {}", url);
      }

      long sleepTime = INITIAL_SLEEPING_TIME_IN_MILLISECONDS;
      DecorrelatedJitterBackoff backoff =
          new DecorrelatedJitterBackoff(sleepTime, MAX_SLEEPING_TIME_IN_MILLISECONDS);
      boolean success = false;

      final int maxRetryCounter = isOCSPFailOpen() ? 1 : 2;
      Exception savedEx = null;
      CloseableHttpClient httpClient =
          ocspCacheServerClient.computeIfAbsent(
              getOCSPResponderConnectionTimeout(),
              k -> getHttpClient(getOCSPResponderConnectionTimeout()));

      for (int retry = 0; retry < maxRetryCounter; ++retry) {
        try {
          if (!ocspCacheServer.new_endpoint_enabled) {
            HttpGet get = new HttpGet(url.toString());
            response = httpClient.execute(get);
          } else {
            HttpPost post = new HttpPost(url.toString());
            post.setHeader("Content-Type", "application/json");
            OCSPPostReqData postReqData =
                new OCSPPostReqData(ocspUrlStr, ocspReqDerBase64, cid_enc, hname);
            String json_payload = OBJECT_MAPPER.writeValueAsString(postReqData);
            post.setEntity(new StringEntity(json_payload, "utf-8"));
            response = httpClient.execute(post);
          }
          success =
              response != null && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
          if (success) {
            break;
          }
        } catch (IOException ex) {
          logger.debug("Failed to reach out OCSP responder: {}", ex.getMessage());
          savedEx = ex;
        }
        IOUtils.closeQuietly(response);

        logger.debug("Retrying {}/{} after sleeping {} ms", retry + 1, maxRetryCounter, sleepTime);
        try {
          if (retry + 1 < maxRetryCounter) {
            Thread.sleep(sleepTime);
            sleepTime = backoff.nextSleepTime(sleepTime);
          }
        } catch (InterruptedException ex0) { // nop
        }
      }
      if (!success) {
        throw new CertificateEncodingException(
            String.format(
                "Failed to get OCSP response. StatusCode: %d, URL: %s",
                response == null ? null : response.getStatusLine().getStatusCode(), ocspUrlStr),
            savedEx);
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copy(response.getEntity().getContent(), out);
      OCSPResp ocspResp = new OCSPResp(out.toByteArray());
      out.close();
      if (ocspResp.getStatus() != OCSPResp.SUCCESSFUL) {
        throw new CertificateEncodingException(
            String.format(
                "Failed to get OCSP response. Status: %s",
                OCSP_RESPONSE_CODE_TO_STRING.get(ocspResp.getStatus())));
      }

      return ocspResp;
    } catch (IOException ex) {
      throw new CertificateEncodingException("Failed to encode object.", ex);
    } finally {
      IOUtils.closeQuietly(response);
    }
  }

  private void checkExistOCSPURL(Set<String> ocspUrls) throws CertificateEncodingException {
    if (ocspUrls.size() == 0 || isEnabledSystemTestParameter(SF_OCSP_TEST_NO_OCSP_RESPONDER_URL)) {
      throw new CertificateEncodingException(
          "No OCSP Responder URL is attached to the certificate.",
          new SFOCSPException(
              OCSPErrorCode.NO_OCSP_URL_ATTACHED,
              "No OCSP Responder URL is attached to the certificate."));
    }
  }

  private int getOCSPResponderConnectionTimeout() {
    int timeout = DEFAULT_OCSP_RESPONDER_CONNECTION_TIMEOUT;
    if (systemGetProperty(SF_OCSP_TEST_OCSP_RESPONDER_TIMEOUT) != null) {
      try {
        timeout = Integer.parseInt(systemGetProperty(SF_OCSP_TEST_OCSP_RESPONDER_TIMEOUT));
      } catch (Exception ex) {
        // nop
      }
    }
    return timeout;
  }

  private String overrideOCSPURL(String ocspURL) {
    String ocspURLInput = systemGetProperty(SF_OCSP_TEST_RESPONDER_URL);
    if (ocspURLInput != null) {
      logger.debug("Overriding OCSP url to: {}", ocspURLInput);
      return ocspURLInput;
    }
    logger.debug("Overriding OCSP url to: {}", ocspURL);
    return ocspURL;
  }

  /**
   * Validates the certificate revocation status
   *
   * @param pairIssuerSubject a pair of issuer and subject certificates
   * @param ocspRespB64 Base64 encoded OCSP Response object
   * @throws SFOCSPException raises if any other error occurs
   */
  private void validateRevocationStatusMain(
      SFPair<Certificate, Certificate> pairIssuerSubject, String ocspRespB64)
      throws SFOCSPException {
    try {
      OCSPResp ocspResp = b64ToOCSPResp(ocspRespB64);
      if (ocspResp == null) {
        throw new SFOCSPException(
            OCSPErrorCode.INVALID_OCSP_RESPONSE, "OCSP response is null. The content is invalid.");
      }
      Date currentTime = new Date();
      BasicOCSPResp basicOcspResp = (BasicOCSPResp) (ocspResp.getResponseObject());
      X509CertificateHolder[] attachedCerts = basicOcspResp.getCerts();
      X509CertificateHolder signVerifyCert;
      checkInvalidSigningCertTestParameter();
      if (attachedCerts.length > 0) {
        logger.debug(
            "Certificate is attached for verification. "
                + "Verifying it by the issuer certificate.",
            false);
        signVerifyCert = attachedCerts[0];
        if (currentTime.after(signVerifyCert.getNotAfter())
            || currentTime.before(signVerifyCert.getNotBefore())) {
          throw new SFOCSPException(
              OCSPErrorCode.EXPIRED_OCSP_SIGNING_CERTIFICATE,
              String.format(
                  "Cert attached to "
                      + "OCSP Response is invalid."
                      + "Current time - %s"
                      + "Certificate not before time - %s"
                      + "Certificate not after time - %s",
                  currentTime, signVerifyCert.getNotBefore(), signVerifyCert.getNotAfter()));
        }
        try {
          verifySignature(
              new X509CertificateHolder(pairIssuerSubject.left.getEncoded()),
              signVerifyCert.getSignature(),
              CONVERTER_X509.getCertificate(signVerifyCert).getTBSCertificate(),
              signVerifyCert.getSignatureAlgorithm());
        } catch (CertificateException ex) {
          logger.debug("OCSP Signing Certificate signature verification failed", false);
          throw new SFOCSPException(
              OCSPErrorCode.INVALID_CERTIFICATE_SIGNATURE,
              "OCSP Signing Certificate signature verification failed",
              ex);
        }
        logger.debug("Verifying OCSP signature by the attached certificate public key.", false);
      } else {
        logger.debug(
            "Certificate is NOT attached for verification. "
                + "Verifying OCSP signature by the issuer public key.",
            false);
        signVerifyCert = new X509CertificateHolder(pairIssuerSubject.left.getEncoded());
      }
      try {
        verifySignature(
            signVerifyCert,
            basicOcspResp.getSignature(),
            basicOcspResp.getTBSResponseData(),
            basicOcspResp.getSignatureAlgorithmID());
      } catch (CertificateException ex) {
        logger.debug("OCSP signature verification failed", false);
        throw new SFOCSPException(
            OCSPErrorCode.INVALID_OCSP_RESPONSE_SIGNATURE,
            "OCSP signature verification failed",
            ex);
      }

      validateBasicOcspResponse(currentTime, basicOcspResp);
    } catch (IOException | OCSPException ex) {
      throw new SFOCSPException(
          OCSPErrorCode.REVOCATION_CHECK_FAILURE, "Failed to check revocation status.", ex);
    }
  }

  private void checkInvalidSigningCertTestParameter() throws SFOCSPException {
    if (isEnabledSystemTestParameter(SF_OCSP_TEST_INVALID_SIGNING_CERT)) {
      throw new SFOCSPException(
          OCSPErrorCode.EXPIRED_OCSP_SIGNING_CERTIFICATE,
          "Cert attached to OCSP Response is invalid");
    }
  }

  /**
   * Validates OCSP Basic OCSP response.
   *
   * @param currentTime the current timestamp.
   * @param basicOcspResp BasicOcspResponse data.
   * @throws SFOCSPException raises if any failure occurs.
   */
  private void validateBasicOcspResponse(Date currentTime, BasicOCSPResp basicOcspResp)
      throws SFOCSPException {
    for (SingleResp singleResps : basicOcspResp.getResponses()) {
      checkCertUnknownTestParameter();
      CertificateStatus certStatus = singleResps.getCertStatus();
      if (certStatus != CertificateStatus.GOOD) {
        if (certStatus instanceof RevokedStatus) {
          RevokedStatus status = (RevokedStatus) certStatus;
          int reason;
          try {
            reason = status.getRevocationReason();
          } catch (IllegalStateException ex) {
            reason = -1;
          }
          Date revocationTime = status.getRevocationTime();
          throw new SFOCSPException(
              OCSPErrorCode.CERTIFICATE_STATUS_REVOKED,
              String.format(
                  "The certificate has been revoked. Reason: %d, Time: %s",
                  reason, DATE_FORMAT_UTC.format(revocationTime)));
        } else {
          // Unknown status
          throw new SFOCSPException(
              OCSPErrorCode.CERTIFICATE_STATUS_UNKNOWN,
              "Failed to validate the certificate for UNKNOWN reason.");
        }
      }

      Date thisUpdate = singleResps.getThisUpdate();
      Date nextUpdate = singleResps.getNextUpdate();
      logger.debug(
          "Current Time: {}, This Update: {}, Next Update: {}",
          currentTime,
          thisUpdate,
          nextUpdate);
      if (!isValidityRange(currentTime, thisUpdate, nextUpdate)) {
        throw new SFOCSPException(
            OCSPErrorCode.INVALID_OCSP_RESPONSE_VALIDITY,
            String.format(
                "The OCSP response validity is out of range: "
                    + "Current Time: %s, This Update: %s, Next Update: %s",
                DATE_FORMAT_UTC.format(currentTime),
                DATE_FORMAT_UTC.format(thisUpdate),
                DATE_FORMAT_UTC.format(nextUpdate)));
      }
    }
    logger.debug("OK. Verified the certificate revocation status.", false);
  }

  private void checkCertUnknownTestParameter() throws SFOCSPException {
    if (isEnabledSystemTestParameter(SF_OCSP_TEST_INJECT_UNKNOWN_STATUS)) {
      throw new SFOCSPException(
          OCSPErrorCode.CERTIFICATE_STATUS_UNKNOWN,
          "Failed to validate the certificate for UNKNOWN reason.");
    }
  }

  /**
   * Creates a OCSP Request
   *
   * @param pairIssuerSubject a pair of issuer and subject certificates
   * @return OCSPReq object
   */
  private OCSPReq createRequest(SFPair<Certificate, Certificate> pairIssuerSubject)
      throws IOException {
    Certificate issuer = pairIssuerSubject.left;
    Certificate subject = pairIssuerSubject.right;
    OCSPReqBuilder gen = new OCSPReqBuilder();
    try {
      DigestCalculator digest = new SHA1DigestCalculator();
      X509CertificateHolder certHolder = new X509CertificateHolder(issuer.getEncoded());
      CertificateID certId =
          new CertificateID(digest, certHolder, subject.getSerialNumber().getValue());
      gen.addRequest(certId);
      return gen.build();
    } catch (OCSPException ex) {
      throw new IOException("Failed to build a OCSPReq.", ex);
    }
  }

  /**
   * Converts X509Certificate to Bouncy Castle Certificate
   *
   * @param chain an array of X509Certificate
   * @return a list of Bouncy Castle Certificate
   */
  private List<Certificate> convertToBouncyCastleCertificate(X509Certificate[] chain)
      throws CertificateEncodingException {
    final List<Certificate> bcChain = new ArrayList<>();
    for (X509Certificate cert : chain) {
      bcChain.add(Certificate.getInstance(cert.getEncoded()));
    }
    return bcChain;
  }

  /**
   * Creates a pair of Issuer and Subject certificates
   *
   * @param bcChain a list of bouncy castle Certificate
   * @return a list of pair of Issuer and Subject certificates
   */
  private List<SFPair<Certificate, Certificate>> getPairIssuerSubject(List<Certificate> bcChain)
      throws CertificateException {
    List<SFPair<Certificate, Certificate>> pairIssuerSubject = new ArrayList<>();
    for (int i = 0, len = bcChain.size(); i < len; ++i) {
      Certificate bcCert = bcChain.get(i);
      if (bcCert.getIssuer().equals(bcCert.getSubject())) {
        continue; // skipping ROOT CA
      }
      if (i < len - 1) {
        pairIssuerSubject.add(SFPair.of(bcChain.get(i + 1), bcChain.get(i)));
      } else {
        // no root CA certificate is attached in the certificate chain, so
        // getting one from the root CA from JVM.
        Certificate issuer = ROOT_CA.get(bcCert.getIssuer().hashCode());
        if (issuer == null) {
          throw new CertificateException(
              "Failed to find the root CA.",
              new SFOCSPException(OCSPErrorCode.NO_ROOTCA_FOUND, "Failed to find the root CA."));
        }
        pairIssuerSubject.add(SFPair.of(issuer, bcChain.get(i)));
      }
    }
    return pairIssuerSubject;
  }

  /**
   * Gets OCSP URLs associated with the certificate.
   *
   * @param bcCert Bouncy Castle Certificate
   * @return a set of OCSP URLs
   */
  private Set<String> getOcspUrls(Certificate bcCert) throws IOException {
    TBSCertificate bcTbsCert = bcCert.getTBSCertificate();
    Extensions bcExts = bcTbsCert.getExtensions();
    if (bcExts == null) {
      throw new IOException("Failed to get Tbs Certificate.");
    }

    Set<String> ocsp = new HashSet<>();
    for (Enumeration<?> en = bcExts.oids(); en.hasMoreElements(); ) {
      ASN1ObjectIdentifier oid = (ASN1ObjectIdentifier) en.nextElement();
      Extension bcExt = bcExts.getExtension(oid);
      if (Extension.authorityInfoAccess.equals(bcExt.getExtnId())) {
        // OCSP URLS are included in authorityInfoAccess
        DLSequence seq = (DLSequence) bcExt.getParsedValue();
        for (ASN1Encodable asn : seq) {
          ASN1Encodable[] pairOfAsn = ((DLSequence) asn).toArray();
          if (pairOfAsn.length == 2) {
            ASN1ObjectIdentifier key = (ASN1ObjectIdentifier) pairOfAsn[0];
            if (OIDocsp.equals(key)) {
              // ensure OCSP and not CRL
              GeneralName gn = GeneralName.getInstance(pairOfAsn[1]);
              ocsp.add(gn.getName().toString());
            }
          }
        }
      }
    }
    return ocsp;
  }

  /** OCSP Response Utils */
  private String ocspResponseToB64(OCSPResp ocspResp) {
    if (ocspResp == null) {
      return null;
    }
    try {
      return Base64.encodeBase64String(ocspResp.getEncoded());
    } catch (Throwable ex) {
      logger.debug("Could not convert OCSP Response to Base64", false);
      return null;
    }
  }

  private OCSPResp b64ToOCSPResp(String ocspRespB64) {
    try {
      return new OCSPResp(Base64.decodeBase64(ocspRespB64));
    } catch (Throwable ex) {
      logger.debug("Could not cover OCSP Response from Base64 to OCSPResp object", false);
      return null;
    }
  }

  static class OCSPCacheServer {
    String SF_OCSP_RESPONSE_CACHE_SERVER;
    String SF_OCSP_RESPONSE_RETRY_URL;
    boolean new_endpoint_enabled;

    void resetOCSPResponseCacheServer(String host) {
      String ocspCacheServerUrl;
      if (host.toLowerCase().contains(".global.snowflakecomputing.")) {
        ocspCacheServerUrl =
            String.format("https://ocspssd%s/%s", host.substring(host.indexOf('-')), "ocsp");
      } else if (host.toLowerCase().contains(".snowflakecomputing.")) {
        ocspCacheServerUrl =
            String.format("https://ocspssd%s/%s", host.substring(host.indexOf('.')), "ocsp");
      } else {
        String topLevelDomain = host.substring(host.lastIndexOf(".") + 1);
        ocspCacheServerUrl =
            String.format("https://ocspssd.snowflakecomputing.%s/ocsp", topLevelDomain);
      }
      SF_OCSP_RESPONSE_CACHE_SERVER = String.format("%s/%s", ocspCacheServerUrl, "fetch");
      SF_OCSP_RESPONSE_RETRY_URL = String.format("%s/%s", ocspCacheServerUrl, "retry");
    }
  }

  private static class OCSPPostReqData {
    private String ocsp_url;
    private String ocsp_req;
    private String cert_id_enc;
    private String hostname;

    OCSPPostReqData(String ocsp_url, String ocsp_req, String cert_id_enc, String hname) {
      this.ocsp_url = ocsp_url;
      this.ocsp_req = ocsp_req;
      this.cert_id_enc = cert_id_enc;
      this.hostname = hname;
    }
  }

  /** OCSP response cache key object */
  static class OcspResponseCacheKey {
    final byte[] nameHash;
    final byte[] keyHash;
    final BigInteger serialNumber;

    OcspResponseCacheKey(byte[] nameHash, byte[] keyHash, BigInteger serialNumber) {
      this.nameHash = nameHash;
      this.keyHash = keyHash;
      this.serialNumber = serialNumber;
    }

    public int hashCode() {
      int ret = Arrays.hashCode(this.nameHash) * 37;
      ret = ret * 10 + Arrays.hashCode(this.keyHash) * 37;
      ret = ret * 10 + this.serialNumber.hashCode();
      return ret;
    }

    public boolean equals(Object obj) {
      if (!(obj instanceof OcspResponseCacheKey)) {
        return false;
      }
      OcspResponseCacheKey target = (OcspResponseCacheKey) obj;
      return Arrays.equals(this.nameHash, target.nameHash)
          && Arrays.equals(this.keyHash, target.keyHash)
          && this.serialNumber.equals(target.serialNumber);
    }

    public String toString() {
      return String.format(
          "OcspResponseCacheKey: NameHash: %s, KeyHash: %s, SerialNumber: %s",
          HexUtil.byteToHexString(nameHash),
          HexUtil.byteToHexString(keyHash),
          serialNumber.toString());
    }
  }

  /** SHA1 Digest Calculator used in OCSP Req. */
  static class SHA1DigestCalculator implements DigestCalculator {
    private ByteArrayOutputStream bOut = new ByteArrayOutputStream();

    public AlgorithmIdentifier getAlgorithmIdentifier() {
      return new AlgorithmIdentifier(OIWObjectIdentifiers.idSHA1);
    }

    public OutputStream getOutputStream() {
      return bOut;
    }

    public byte[] getDigest() {
      byte[] bytes = bOut.toByteArray();
      bOut.reset();
      try {
        MessageDigest messageDigest = MessageDigest.getInstance(ALGORITHM_SHA1_NAME);
        return messageDigest.digest(bytes);
      } catch (NoSuchAlgorithmException ex) {
        String errMsg =
            String.format(
                "Failed to instantiate the algorithm: %s. err=%s",
                ALGORITHM_SHA1_NAME, ex.getMessage());
        logger.error(errMsg, false);
        throw new RuntimeException(errMsg);
      }
    }
  }
}
