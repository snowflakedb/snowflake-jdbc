/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SFPair;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
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
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.DigestCalculator;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
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
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;


/**
 * SFTrustManager is a composite of TrustManager of the default JVM
 * TrustManager and Snowflake OCSP revocation status checker. Use this
 * when initializing SSLContext object.
 *
 * <pre>
 * {@code
 * TrustManager[] trustManagers = {new SFTrustManager()};
 * SSLContext sslContext = SSLContext.getInstance("TLS");
 * sslContext.init(null, trustManagers, null);
 * }
 * </pre>
 */
class SFTrustManager implements X509TrustManager
{
  private static final
  SFLogger LOGGER = SFLoggerFactory.getLogger(SFTrustManager.class);

  private static final ASN1ObjectIdentifier OIDocsp = new ASN1ObjectIdentifier("1.3.6.1.5.5.7.48.1").intern();

  private static final ASN1ObjectIdentifier SHA1RSA = new ASN1ObjectIdentifier("1.2.840.113549.1.1.5").intern();
  private static final ASN1ObjectIdentifier SHA256RSA = new ASN1ObjectIdentifier("1.2.840.113549.1.1.11").intern();
  private static final ASN1ObjectIdentifier SHA384RSA = new ASN1ObjectIdentifier("1.2.840.113549.1.1.12").intern();
  private static final ASN1ObjectIdentifier SHA512RSA = new ASN1ObjectIdentifier("1.2.840.113549.1.1.13").intern();
  /**
   * Object mapper for JSON encoding and decoding
   */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * OCSP response cache entry expiration time (s)
   */
  private static final long CACHE_EXPIRATION = 86400L;

  /**
   * OCSP response cache lock file expiration time (ms)
   */
  private static final long CACHE_FILE_LOCK_EXPIRATION = 60000L;

  /**
   * Default OCSP Cache server host name
   */
  public static final String DEFAULT_OCSP_CACHE_HOST = "http://ocsp.snowflakecomputing.com";

  /**
   * OCSP response cache file name. Should be identical to other driver's
   * cache file name.
   */
  public static final String OCSP_CACHE_FILE_NAME = "ocsp_response_cache.json";

  /**
   * OCSP response file cache directory
   */
  private static final FileCacheManager fileCacheManager;

  static
  {
    fileCacheManager = FileCacheManager
        .builder()
        .setCacheDirectoryEnvironmentVariable("SF_OCSP_RESPONSE_CACHE_DIR")
        .setBaseCacheFileName(OCSP_CACHE_FILE_NAME)
        .setCacheExpiration(CACHE_EXPIRATION)
        .setCacheFileLockExpiration(CACHE_FILE_LOCK_EXPIRATION).build();
  }

  /**
   * OCSP response cache server URL.
   */
  private static String SF_OCSP_RESPONSE_CACHE_SERVER_URL = String.format(
      "%s/%s", DEFAULT_OCSP_CACHE_HOST, OCSP_CACHE_FILE_NAME);

  /**
   * OCSP Response Cache server Retry URL pattern
   */
  private static String SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN;

  /**
   * Tolerable validity date range ratio.
   */
  private static final float TOLERABLE_VALIDITY_RANGE_RATIO = 0.01f;

  /**
   * Maximum clocktime skew (ms)
   */
  private static final long MAX_CLOCK_SKEW = 900000L;

  /**
   * Minimum cache warm up time (ms)
   */
  private static final long MIN_CACHE_WARMUP_TIME = 18000000L;

  /**
   * Maximum retry counter (times)
   */
  private static final int MAX_RETRY_COUNTER = 10;

  /**
   * Initial sleeping time in retry (ms)
   */
  private static final long INITIAL_SLEEPING_TIME = 1000L;
  /**
   * Maximum sleeping time in retry (ms)
   */
  private static final long MAX_SLEEPING_TIME = 16000L;

  /**
   * Map from signature algorithm ASN1 object to the name.
   */
  private static final Map<ASN1ObjectIdentifier, String> SIGNATURE_OID_TO_STRING = new HashMap<>();

  static
  {
    SIGNATURE_OID_TO_STRING.put(SHA1RSA, "SHA1withRSA");
    SIGNATURE_OID_TO_STRING.put(SHA256RSA, "SHA256withRSA");
    SIGNATURE_OID_TO_STRING.put(SHA384RSA, "SHA384withRSA");
    SIGNATURE_OID_TO_STRING.put(SHA512RSA, "SHA512withRSA");
  }

  /**
   * Map from OCSP response code to a string representation.
   */
  private static final Map<Integer, String> OCSP_RESPONSE_CODE_TO_STRING = new HashMap<>();

  static
  {
    OCSP_RESPONSE_CODE_TO_STRING.put(OCSPResp.SUCCESSFUL, "successful");
    OCSP_RESPONSE_CODE_TO_STRING.put(OCSPResp.MALFORMED_REQUEST, "malformedRequest");
    OCSP_RESPONSE_CODE_TO_STRING.put(OCSPResp.INTERNAL_ERROR, "internalError");
    OCSP_RESPONSE_CODE_TO_STRING.put(OCSPResp.TRY_LATER, "tryLater");
    OCSP_RESPONSE_CODE_TO_STRING.put(OCSPResp.SIG_REQUIRED, "sigRequired");
    OCSP_RESPONSE_CODE_TO_STRING.put(OCSPResp.UNAUTHORIZED, "unauthorized");
  }

  static
  {
    // Add Bouncy Castle to the security provider. This is required to
    // verify the signature on OCSP response and attached certificates.
    Security.addProvider(new BouncyCastleProvider());
  }

  private static JcaX509CertificateConverter CONVERTER_X509 = new JcaX509CertificateConverter();

  /**
   * RootCA cache
   */
  private static Map<Integer, Certificate> ROOT_CA = new HashMap<>();
  private final static Object ROOT_CA_LOCK = new Object();

  /**
   * OCSP Response cache
   */
  private final static Map<OcspResponseCacheKey, SFPair<Long, OCSPResp>> OCSP_RESPONSE_CACHE = new HashMap<>();
  private final static Object OCSP_RESPONSE_CACHE_LOCK = new Object();
  private static boolean WAS_CACHE_UPDATED = false;
  private static boolean WAS_CACHE_READ = false;

  /**
   * Date and timestamp format
   */
  private final static SimpleDateFormat DATE_FORMAT_UTC = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  static
  {
    DATE_FORMAT_UTC.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  /**
   * The default JVM Trust manager.
   */
  private final X509TrustManager trustManager;

  /**
   * Use OCSP response cache server?
   */
  private final boolean useOcspResponseCacheServer;

  /**
   * Constructor with the cache file. If not specified, the default cachefile
   * is used.
   *
   * @param cacheFile                  cache file.
   * @param useOcspResponseCacheServer true if use OCSP response cache server is used.
   */
  SFTrustManager(File cacheFile, boolean useOcspResponseCacheServer)
  {
    this.trustManager = getTrustManager(
        KeyManagerFactory.getDefaultAlgorithm());

    synchronized (OCSP_RESPONSE_CACHE_LOCK)
    {
      if (cacheFile != null)
      {
        fileCacheManager.overrideCacheFile(cacheFile);
      }
      if (!WAS_CACHE_READ)
      {
        // read cache file once
        JsonNode res = fileCacheManager.readCacheFile();
        readJsonStoreCache(res);
        WAS_CACHE_READ = true;
      }
    }
    this.useOcspResponseCacheServer = useOcspResponseCacheServer;
  }

  /**
   * Reset OCSP Cache server URL
   *
   * @param ocspCacheServerUrl OCSP Cache server URL
   */
  static void resetOCSPResponseCacherServerURL(String ocspCacheServerUrl)
  {
    if (ocspCacheServerUrl == null || SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN != null)
    {
      return;
    }
    SF_OCSP_RESPONSE_CACHE_SERVER_URL = ocspCacheServerUrl;
    if (!SF_OCSP_RESPONSE_CACHE_SERVER_URL.startsWith(DEFAULT_OCSP_CACHE_HOST))
    {
      try
      {
        URL url = new URL(SF_OCSP_RESPONSE_CACHE_SERVER_URL);
        if (url.getPort() > 0)
        {
          SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN =
              String.format("%s://%s:%d/retry/",
                  url.getProtocol(), url.getHost(), url.getPort()) + "%s/%s";
        }
        else
        {
          SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN =
              String.format("%s://%s/retry/",
                  url.getProtocol(), url.getHost()) + "%s/%s";
        }
      }
      catch (IOException e)
      {
        throw new RuntimeException(
            String.format(
                "Failed to parse SF_OCSP_RESPONSE_CACHE_SERVER_URL: %s",
                SF_OCSP_RESPONSE_CACHE_SERVER_URL));
      }
    }
    else
    {
      // default OCSP doesn't support retry endpoint yet
      SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
    }
  }

  /**
   * Get TrustManager for the algorithm.
   * This is mainly used to get the JVM default trust manager and
   * cache all of the root CA.
   *
   * @param algorithm algorithm.
   * @return TrustManager object.
   */
  private X509TrustManager getTrustManager(String algorithm)
  {
    try
    {
      TrustManagerFactory factory = TrustManagerFactory.getInstance(algorithm);
      factory.init((KeyStore) null);
      X509TrustManager ret = null;
      for (TrustManager tm : factory.getTrustManagers())
      {
        // Multiple TrustManager may be attached. We just need X509 Trust
        // Manager here.
        if (tm instanceof X509TrustManager)
        {
          ret = (X509TrustManager) tm;
          break;
        }
      }
      if (ret == null)
      {
        return null;
      }
      synchronized (ROOT_CA_LOCK)
      {
        // cache root CA certificates for later use.
        if (ROOT_CA.size() == 0)
        {
          for (X509Certificate cert : ret.getAcceptedIssuers())
          {
            Certificate bcCert = Certificate.getInstance(cert.getEncoded());
            ROOT_CA.put(bcCert.getSubject().hashCode(), bcCert);
          }
        }
      }
      return ret;
    }
    catch (NoSuchAlgorithmException | KeyStoreException | CertificateEncodingException ex)
    {
      throw new SSLInitializationException(ex.getMessage(), ex);
    }
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException
  {
    // default behavior
    trustManager.checkClientTrusted(chain, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException
  {
    trustManager.checkServerTrusted(chain, authType);
    this.validateRevocationStatus(chain);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers()
  {
    return trustManager.getAcceptedIssuers();
  }

  /**
   * Certificate Revocation checks
   *
   * @param chain chain of certificates attached.
   * @throws CertificateException if any certificate validation fails
   */
  void validateRevocationStatus(X509Certificate[] chain) throws CertificateException
  {
    final List<Certificate> bcChain = convertToBouncyCastleCertificate(chain);
    final List<SFPair<Certificate, Certificate>> pairIssuerSubjectList =
        getPairIssuerSubject(bcChain);
    synchronized (OCSP_RESPONSE_CACHE_LOCK)
    {
      boolean isCached = isCached(pairIssuerSubjectList);
      if (this.useOcspResponseCacheServer && !isCached)
      {
        LOGGER.debug(
            "Downloading OCSP response cache from the server. URL: {}",
            SF_OCSP_RESPONSE_CACHE_SERVER_URL);
        readOcspResponseCacheServer();
        // if the cache is downloaded from the server, it should be written
        // to the file cache at all times.
        WAS_CACHE_UPDATED = true;
      }
      executeRevocationStatusChecks(pairIssuerSubjectList);
      if (WAS_CACHE_UPDATED)
      {
        JsonNode input = encodeCacheToJSON();
        fileCacheManager.writeCacheFile(input);
        WAS_CACHE_UPDATED = false;
      }
    }
  }

  /**
   * Executes the revocation status checks for all chained certificates
   *
   * @param pairIssuerSubjectList a list of pair of issuer and subject certificates.
   * @throws CertificateException raises if any error occurs.
   */
  private void executeRevocationStatusChecks(
      List<SFPair<Certificate, Certificate>> pairIssuerSubjectList)
      throws CertificateException
  {
    long currentTimeSecond = new Date().getTime() / 1000L;
    try
    {
      for (SFPair<Certificate, Certificate> pairIssuerSubject : pairIssuerSubjectList)
      {
        executeOneRevoctionStatusCheck(pairIssuerSubject, currentTimeSecond);
      }
    }
    catch (IOException ex)
    {
      LOGGER.debug("Failed to decode CertID. Ignored.");
    }
  }

  /**
   * Executes a single revocation status check
   *
   * @param pairIssuerSubject a pair of issuer and subject certificate
   * @param currentTimeSecond the current timestamp
   * @throws IOException          raises if encoding fails.
   * @throws CertificateException if certificate exception is raised.
   */
  private void executeOneRevoctionStatusCheck(
      SFPair<Certificate, Certificate> pairIssuerSubject, long currentTimeSecond)
      throws IOException, CertificateException
  {
    OCSPReq req = createRequest(pairIssuerSubject);
    CertID cid = req.getRequestList()[0].getCertID().toASN1Primitive();
    OcspResponseCacheKey keyOcspResponse = new OcspResponseCacheKey(
        cid.getIssuerNameHash().getEncoded(),
        cid.getIssuerKeyHash().getEncoded(),
        cid.getSerialNumber().getValue());

    long sleepTime = INITIAL_SLEEPING_TIME;
    CertificateException error = null;
    boolean success = false;
    for (int retry = 0; retry < MAX_RETRY_COUNTER; ++retry)
    {
      SFPair<Long, OCSPResp> value0 = OCSP_RESPONSE_CACHE.get(keyOcspResponse);
      OCSPResp ocspResp;
      try
      {
        if (value0 == null)
        {
          LOGGER.debug("not hit cache.");
          ocspResp = fetchOcspResponse(pairIssuerSubject, req);
          OCSP_RESPONSE_CACHE.put(
              keyOcspResponse, SFPair.of(currentTimeSecond, ocspResp));
          WAS_CACHE_UPDATED = true;
        }
        else
        {
          LOGGER.debug("hit cache.");
          ocspResp = value0.right;
        }
        LOGGER.debug("validating. {}",
            CertificateIDToString(req.getRequestList()[0].getCertID()));
        validateRevocationStatusMain(pairIssuerSubject, ocspResp);
        success = true;
        break;
      }
      catch (CertificateException ex)
      {
        if (OCSP_RESPONSE_CACHE.containsKey(keyOcspResponse))
        {
          LOGGER.debug("deleting the invalid OCSP cache.");
          OCSP_RESPONSE_CACHE.remove(keyOcspResponse);
          WAS_CACHE_UPDATED = true;
        }
        error = ex;
        LOGGER.debug("Retrying {}/{} after sleeping {}(ms)",
            retry + 1, MAX_RETRY_COUNTER, sleepTime);
        try
        {
          Thread.sleep(sleepTime);
          sleepTime = minLong(MAX_SLEEPING_TIME, sleepTime * 2);
        }
        catch (InterruptedException ex0)
        { // nop
        }
      }
    }
    if (!success)
    {
      // still not success, raise an error.
      throw error;
    }
  }

  /**
   * Is OCSP Response cached?
   *
   * @param pairIssuerSubjectList a list of pair of issuer and subject certificates
   * @return true if all of OCSP response are cached else false
   */
  private boolean isCached(List<SFPair<Certificate, Certificate>> pairIssuerSubjectList)
  {
    long currentTimeSecond = new Date().getTime() / 1000L;
    boolean isCached = true;
    try
    {
      for (SFPair<Certificate, Certificate> pairIssuerSubject : pairIssuerSubjectList)
      {
        OCSPReq req = createRequest(pairIssuerSubject);
        CertificateID certificateId = req.getRequestList()[0].getCertID();
        LOGGER.debug(CertificateIDToString(certificateId));
        CertID cid = certificateId.toASN1Primitive();
        OcspResponseCacheKey k = new OcspResponseCacheKey(
            cid.getIssuerNameHash().getEncoded(),
            cid.getIssuerKeyHash().getEncoded(),
            cid.getSerialNumber().getValue());

        SFPair<Long, OCSPResp> res = OCSP_RESPONSE_CACHE.get(k);
        if (res == null)
        {
          LOGGER.debug("Not all OCSP responses for the certificate is in the cache.");
          isCached = false;
          break;
        }
        else if (currentTimeSecond - CACHE_EXPIRATION > res.left)
        {
          LOGGER.debug("Cache for CertID expired.");
          isCached = false;
          break;
        }
      }
    }
    catch (IOException ex)
    {
      LOGGER.debug("Failed to encode CertID.");
    }
    return isCached;
  }


  /**
   * CertificateID to string
   *
   * @param certificateID CertificateID
   * @return a string representation of CertificateID
   */
  private static String CertificateIDToString(CertificateID certificateID)
  {
    return String.format("CertID. NameHash: %s, KeyHash: %s, Serial Number: %s",
        byteToHexString(certificateID.getIssuerNameHash()),
        byteToHexString(certificateID.getIssuerKeyHash()),
        MessageFormat.format("{0,number,#}",
            certificateID.getSerialNumber()));

  }

  /**
   * Decodes OCSP Response Cache key from JSON
   *
   * @param elem A JSON element
   * @return OcspResponseCacheKey object
   */
  private static SFPair<OcspResponseCacheKey, SFPair<Long, OCSPResp>>
  decodeCacheFromJSON(Map.Entry<String, JsonNode> elem) throws IOException
  {
    long currentTimeSecond = new Date().getTime() / 1000;
    byte[] certIdDer = Base64.decodeBase64(elem.getKey());
    DLSequence rawCertId = (DLSequence) ASN1ObjectIdentifier.fromByteArray(certIdDer);
    ASN1Encodable[] rawCertIdArray = rawCertId.toArray();
    byte[] issuerNameHashDer = ((DEROctetString) rawCertIdArray[1]).getEncoded();
    byte[] issuerKeyHashDer = ((DEROctetString) rawCertIdArray[2]).getEncoded();
    BigInteger serialNumber = ((ASN1Integer) rawCertIdArray[3]).getValue();

    OcspResponseCacheKey k = new OcspResponseCacheKey(
        issuerNameHashDer, issuerKeyHashDer, serialNumber);

    JsonNode ocspRespBase64 = elem.getValue();
    if (!ocspRespBase64.isArray() || ocspRespBase64.size() != 2)
    {
      LOGGER.debug("Invalid cache file format.");
      return null;
    }
    long producedAt = ocspRespBase64.get(0).asLong();
    byte[] ocspRespDer = Base64.decodeBase64(ocspRespBase64.get(1).asText());
    if (currentTimeSecond - CACHE_EXPIRATION <= producedAt)
    {
      // add cache
      OCSPResp v0 = new OCSPResp(ocspRespDer);
      return SFPair.of(k, SFPair.of(producedAt, v0));
    }
    else
    {
      // delete cache
      return SFPair.of(k, SFPair.of(producedAt, (OCSPResp) null));
    }
  }

  /**
   * Encode OCSP Response Cache to JSON
   *
   * @return JSON object
   */
  private static ObjectNode encodeCacheToJSON()
  {
    try
    {
      ObjectNode out = OBJECT_MAPPER.createObjectNode();
      for (Map.Entry<OcspResponseCacheKey, SFPair<Long, OCSPResp>> elem :
          OCSP_RESPONSE_CACHE.entrySet())
      {
        OcspResponseCacheKey key = elem.getKey();
        SFPair<Long, OCSPResp> value0 = elem.getValue();
        long currentTimeSecond = value0.left;
        OCSPResp value = value0.right;

        DigestCalculator digest = new SHA1DigestCalculator();
        AlgorithmIdentifier algo = digest.getAlgorithmIdentifier();
        ASN1OctetString nameHash = ASN1OctetString.getInstance(key.nameHash);
        ASN1OctetString keyHash = ASN1OctetString.getInstance(key.keyHash);
        ASN1Integer serialNumber = new ASN1Integer(key.serialNumber);
        CertID cid = new CertID(algo, nameHash, keyHash, serialNumber);
        ArrayNode vout = OBJECT_MAPPER.createArrayNode();
        vout.add(currentTimeSecond);
        vout.add(Base64.encodeBase64String(value.getEncoded()));
        out.set(
            Base64.encodeBase64String(cid.toASN1Primitive().getEncoded()),
            vout);
      }
      return out;
    }
    catch (IOException ex)
    {
      LOGGER.debug("Failed to encode ASN1 object.");
    }
    return null;
  }

  /**
   * Reads the OCSP response cache from the server.
   * <p>
   * Must be synchronized by OCSP_RESPONSE_CACHE_LOCK.
   */
  private static void readOcspResponseCacheServer()
  {
    long sleepTime = INITIAL_SLEEPING_TIME;
    Exception error = null;
    for (int retry = 0; retry < MAX_RETRY_COUNTER; ++retry)
    {
      try
      {
        HttpClient client = getHttpClient();
        URI uri = new URI(SF_OCSP_RESPONSE_CACHE_SERVER_URL);
        HttpGet get = new HttpGet(uri);
        HttpResponse response = client.execute(get);

        if (response == null || response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
        {
          throw new IOException(
              String.format(
                  "Failed to get the OCSP response from the OCSP " +
                      "cache server: HTTP: %d",
                  response != null ? response.getStatusLine().getStatusCode() :
                      -1));
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(response.getEntity().getContent(), out);
        JsonNode m = OBJECT_MAPPER.readTree(out.toByteArray());
        readJsonStoreCache(m);
        LOGGER.debug("Successfully downloaded OCSP cache from the server.");
        return;
      }
      catch (IOException | URISyntaxException ex)
      {
        error = ex;
        LOGGER.debug("Retrying {}/{} after sleeping {}(ms)",
            retry + 1, MAX_RETRY_COUNTER, sleepTime);
        try
        {
          Thread.sleep(sleepTime);
          sleepTime = minLong(MAX_SLEEPING_TIME, sleepTime * 2);
        }
        catch (InterruptedException ex0)
        { // nop
        }
      }
    }
    LOGGER.debug(
        "Failed to read the OCSP response cache from the server. " +
            "Server: {}, Err: {}", SF_OCSP_RESPONSE_CACHE_SERVER_URL, error);
  }

  private static void readJsonStoreCache(JsonNode m)
  {
    if (m == null || !m.getNodeType().equals(JsonNodeType.OBJECT))
    {
      LOGGER.debug("Invalid cache file format.");
      return;
    }
    try
    {
      for (Iterator<Map.Entry<String, JsonNode>> itr = m.fields(); itr.hasNext(); )
      {
        SFPair<OcspResponseCacheKey, SFPair<Long, OCSPResp>> ky =
            decodeCacheFromJSON(itr.next());
        if (ky != null && ky.right != null && ky.right.right != null)
        {
          // valid range. cache the result in memory
          OCSP_RESPONSE_CACHE.put(ky.left, ky.right);
        }
        else if (ky != null && OCSP_RESPONSE_CACHE.containsKey(ky.left))
        {
          // delete it from the cache if no OCSP response is back.
          OCSP_RESPONSE_CACHE.remove(ky.left);
          WAS_CACHE_UPDATED = true;
        }
      }
    }
    catch (IOException ex)
    {
      LOGGER.debug("Failed to decode the cache file");
    }
  }

  /**
   * Fetches OCSP response from OCSP server
   *
   * @param pairIssuerSubject a pair of issuer and subject certificates
   * @param req               OCSP Request object
   * @return OCSP Response object
   * @throws CertificateEncodingException if any other error occurs
   */
  private OCSPResp fetchOcspResponse(
      SFPair<Certificate, Certificate> pairIssuerSubject, OCSPReq req)
      throws CertificateEncodingException
  {
    try
    {
      byte[] ocspReqDer = req.getEncoded();
      String ocspReqDerBase64 = Base64.encodeBase64String(ocspReqDer);

      Set<String> ocspUrls = getOcspUrls(pairIssuerSubject.right);
      String ocspUrl = ocspUrls.iterator().next(); // first one
      URL url;
      if (SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN != null)
      {
        url = new URL(SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN.format(
            ocspUrl, ocspReqDerBase64
        ));
      }
      else
      {
        url = new URL(String.format("%s/%s", ocspUrl, ocspReqDerBase64));
      }
      LOGGER.debug(
          "not hit cache. Fetching OCSP response from CA OCSP server. {0}", url.toString());

      long sleepTime = INITIAL_SLEEPING_TIME;
      boolean success = false;
      HttpResponse response = null;

      for (int retry = 0; retry < MAX_RETRY_COUNTER; ++retry)
      {
        HttpClient client = getHttpClient();
        HttpGet get = new HttpGet(url.toString());
        response = client.execute(get);
        if (response != null && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK)
        {
          success = true;
          LOGGER.debug(
              "Successfully downloaded OCSP response from CA server. " +
                  "URL: {}", url.toString());
          break;
        }
        LOGGER.debug("Retrying {}/{} after sleeping {}(ms)",
            retry + 1, MAX_RETRY_COUNTER, sleepTime);
        try
        {
          Thread.sleep(sleepTime);
          sleepTime = minLong(MAX_SLEEPING_TIME, sleepTime * 2);
        }
        catch (InterruptedException ex0)
        { // nop
        }
      }
      if (!success)
      {
        throw new CertificateEncodingException(
            String.format(
                "Failed to get OCSP response. StatusCode: %d, URL: %s",
                response == null ? null : response.getStatusLine().getStatusCode(),
                ocspUrl));
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copy(response.getEntity().getContent(), out);
      OCSPResp ocspResp = new OCSPResp(out.toByteArray());
      if (ocspResp.getStatus() != OCSPResp.SUCCESSFUL)
      {
        throw new CertificateEncodingException(
            String.format("Failed to get OCSP response. Status: %s",
                OCSP_RESPONSE_CODE_TO_STRING.get(ocspResp.getStatus())));
      }

      return ocspResp;
    }
    catch (IOException ex)
    {
      throw new CertificateEncodingException("Failed to encode object.", ex);
    }
  }

  /**
   * Validates the certificate revocation status
   *
   * @param pairIssuerSubject a pair of issuer and subject certificates
   * @param ocspResp          OCSP Response object
   * @throws CertificateException raises if any other error occurs
   */
  private void validateRevocationStatusMain(
      SFPair<Certificate, Certificate> pairIssuerSubject,
      OCSPResp ocspResp) throws CertificateException
  {
    try
    {
      Date currentTime = new Date();
      BasicOCSPResp basicOcspResp = (BasicOCSPResp) (ocspResp.getResponseObject());
      X509CertificateHolder[] attachedCerts = basicOcspResp.getCerts();
      X509CertificateHolder signVerifyCert;
      if (attachedCerts.length > 0)
      {
        LOGGER.debug(
            "Certificate is attached for verification. " +
                "Verifying it by the issuer certificate.");
        signVerifyCert = attachedCerts[0];
        verifySignature(
            new X509CertificateHolder(pairIssuerSubject.left.getEncoded()),
            signVerifyCert.getSignature(),
            CONVERTER_X509.getCertificate(signVerifyCert).getTBSCertificate(),
            signVerifyCert.getSignatureAlgorithm());
        LOGGER.debug(
            "Verifying OCSP signature by the attached certificate public key."
        );
      }
      else
      {
        LOGGER.debug("Certificate is NOT attached for verification. " +
            "Verifying OCSP signature by the issuer public key.");
        signVerifyCert = new X509CertificateHolder(
            pairIssuerSubject.left.getEncoded());
      }
      verifySignature(
          signVerifyCert,
          basicOcspResp.getSignature(),
          basicOcspResp.getTBSResponseData(),
          basicOcspResp.getSignatureAlgorithmID());

      validateBasicOcspResponse(currentTime, basicOcspResp);
    }
    catch (IOException | OCSPException ex)
    {
      throw new CertificateEncodingException(
          "Failed to check revocation status.", ex);
    }
  }

  /**
   * Validates OCSP Basic OCSP response.
   *
   * @param currentTime   the current timestamp.
   * @param basicOcspResp BasicOcspResponse data.
   * @throws CertificateEncodingException raises if any failure occurs.
   */
  private void validateBasicOcspResponse(
      Date currentTime, BasicOCSPResp basicOcspResp)
      throws CertificateEncodingException
  {
    for (SingleResp singleResps : basicOcspResp.getResponses())
    {
      Date thisUpdate = singleResps.getThisUpdate();
      Date nextUpdate = singleResps.getNextUpdate();
      LOGGER.debug("Current Time: {}, This Update: {}, Next Update: {}",
          currentTime, thisUpdate, nextUpdate);
      CertificateStatus certStatus = singleResps.getCertStatus();
      if (certStatus != CertificateStatus.GOOD)
      {
        if (certStatus instanceof RevokedStatus)
        {
          RevokedStatus status = (RevokedStatus) certStatus;
          int reason;
          try
          {
            reason = status.getRevocationReason();
          }
          catch (IllegalStateException ex)
          {
            reason = -1;
          }
          Date revocationTime = status.getRevocationTime();
          throw new CertificateEncodingException(
              String.format(
                  "The certificate has been revoked. Reason: %d, Time: %s",
                  reason, DATE_FORMAT_UTC.format(revocationTime)));
        }
        else
        {
          // Unknown status
          throw new CertificateEncodingException(
              "Failed to validate the certificate for UNKNOWN reason.");
        }
      }
      if (!isValidityRange(currentTime, thisUpdate, nextUpdate))
      {
        throw new CertificateEncodingException(
            String.format(
                "The validity is out of range: " +
                    "Current Time: %s, This Update: %s, Next Update: %s",
                DATE_FORMAT_UTC.format(currentTime),
                DATE_FORMAT_UTC.format(thisUpdate),
                DATE_FORMAT_UTC.format(nextUpdate)));
      }
    }
    LOGGER.debug("OK. Verified the certificate revocation status.");
  }

  /**
   * Verifies the signature of the data
   *
   * @param cert a certificate for public key.
   * @param sig  signature in a byte array.
   * @param data data in a byte array.
   * @param idf  algorithm identifier object.
   * @throws CertificateException raises if the verification fails.
   */
  private static void verifySignature(
      X509CertificateHolder cert,
      byte[] sig, byte[] data, AlgorithmIdentifier idf) throws CertificateException
  {
    try
    {
      String algorithm = SIGNATURE_OID_TO_STRING.get(idf.getAlgorithm());
      if (algorithm == null)
      {
        throw new NoSuchAlgorithmException(
            String.format("Unsupported signature OID. OID: %s", idf));
      }
      Signature signer = Signature.getInstance(
          algorithm, BouncyCastleProvider.PROVIDER_NAME);

      X509Certificate c = CONVERTER_X509.getCertificate(cert);
      signer.initVerify(c.getPublicKey());
      signer.update(data);
      if (!signer.verify(sig))
      {
        throw new CertificateEncodingException(
            String.format("Failed to verify the signature. Potentially the " +
                "data was not generated by by the cert, %s", cert.getSubject()));
      }
    }
    catch (NoSuchAlgorithmException | NoSuchProviderException |
        InvalidKeyException | SignatureException ex)
    {
      throw new CertificateEncodingException(
          "Failed to verify the signature.", ex);
    }
  }

  /**
   * Converts Byte array to hex string
   *
   * @param bytes a byte array
   * @return a string in hexadecimal code
   */
  private static String byteToHexString(byte[] bytes)
  {
    final char[] hexArray = "0123456789ABCDEF".toCharArray();
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++)
    {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = hexArray[v >>> 4];
      hexChars[j * 2 + 1] = hexArray[v & 0x0F];
    }
    return new String(hexChars);
  }

  /**
   * Creates a OCSP Request
   *
   * @param pairIssuerSubject a pair of issuer and subject certificates
   * @return OCSPReq object
   */
  private OCSPReq createRequest(
      SFPair<Certificate, Certificate> pairIssuerSubject)
  {
    Certificate issuer = pairIssuerSubject.left;
    Certificate subject = pairIssuerSubject.right;
    OCSPReqBuilder gen = new OCSPReqBuilder();
    try
    {
      DigestCalculator digest = new SHA1DigestCalculator();
      X509CertificateHolder certHolder = new X509CertificateHolder(issuer.getEncoded());
      CertificateID certId = new CertificateID(
          digest, certHolder, subject.getSerialNumber().getValue());
      gen.addRequest(certId);
      return gen.build();
    }
    catch (OCSPException | IOException ex)
    {
      throw new RuntimeException("Failed to build a OCSPReq.");
    }
  }


  /**
   * Converts X509Certificate to Bouncy Castle Certificate
   *
   * @param chain an array of X509Certificate
   * @return a list of Bouncy Castle Certificate
   */
  private List<Certificate> convertToBouncyCastleCertificate(
      X509Certificate[] chain)
  {
    final List<Certificate> bcChain = new ArrayList<>();
    for (X509Certificate cert : chain)
    {
      try
      {
        bcChain.add(Certificate.getInstance(cert.getEncoded()));
      }
      catch (CertificateEncodingException ex)
      {
        throw new RuntimeException("Failed to decode the certificate DER data");
      }
    }
    return bcChain;
  }

  /**
   * Creates a pair of Issuer and Subject certificates
   *
   * @param bcChain a list of bouncy castle Certificate
   * @return a list of paif of Issuer and Subject certificates
   */
  private List<SFPair<Certificate, Certificate>> getPairIssuerSubject(
      List<Certificate> bcChain)
  {
    List<SFPair<Certificate, Certificate>> pairIssuerSubject = new ArrayList<>();
    for (int i = 0, len = bcChain.size(); i < len; ++i)
    {
      Certificate bcCert = bcChain.get(i);
      if (bcCert.getIssuer().equals(bcCert.getSubject()))
      {
        continue; // skipping ROOT CA
      }
      if (i < len - 1)
      {
        pairIssuerSubject.add(SFPair.of(bcChain.get(i + 1), bcChain.get(i)));
      }
      else
      {
        synchronized (ROOT_CA_LOCK)
        {
          // no root CA certificate is attached in the certificate chain, so
          // getting one from the root CA from JVM.
          Certificate issuer = ROOT_CA.get(bcCert.getIssuer().hashCode());
          if (issuer == null)
          {
            throw new RuntimeException("Failed to find the root CA.");
          }
          pairIssuerSubject.add(SFPair.of(issuer, bcChain.get(i)));
        }
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
  private Set<String> getOcspUrls(Certificate bcCert)
  {
    TBSCertificate bcTbsCert = bcCert.getTBSCertificate();
    Extensions bcExts = bcTbsCert.getExtensions();
    if (bcExts == null)
    {
      throw new RuntimeException("Failed to get Tbs Certificate.");
    }

    Set<String> ocsp = new HashSet<>();
    for (Enumeration en = bcExts.oids(); en.hasMoreElements(); )
    {
      ASN1ObjectIdentifier oid = (ASN1ObjectIdentifier) en.nextElement();
      Extension bcExt = bcExts.getExtension(oid);
      if (bcExt.getExtnId() == Extension.authorityInfoAccess)
      {
        // OCSP URLS are included in authorityInfoAccess
        DLSequence seq = (DLSequence) bcExt.getParsedValue();
        for (ASN1Encodable asn : seq)
        {
          ASN1Encodable[] pairOfAsn = ((DLSequence) asn).toArray();
          if (pairOfAsn.length == 2)
          {
            ASN1ObjectIdentifier key = (ASN1ObjectIdentifier) pairOfAsn[0];
            if (key == OIDocsp)
            {
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

  /**
   * Gets HttpClient object
   *
   * @return HttpClient
   */
  private static HttpClient getHttpClient()
  {
    // using the default HTTP client
    return HttpUtil.getHttpClient();
  }

  private static long minLong(long v1, long v2)
  {
    return v1 < v2 ? v1 : v2;
  }

  private static long maxLong(long v1, long v2)
  {
    return v1 > v2 ? v1 : v2;
  }

  /**
   * Calculates the tolerable validity time beyond the next update.
   * <p>
   * Sometimes CA's OCSP response update is delayed beyond the clock skew
   * as the update is not populated to all OCSP servers for certain period.
   *
   * @param thisUpdate the last update
   * @param nextUpdate the next update
   * @return the tolerable validity beyond the next update.
   */
  private static long calculateTolerableVadility(Date thisUpdate, Date nextUpdate)
  {
    return maxLong((long) ((float) (nextUpdate.getTime() - thisUpdate.getTime()) *
        TOLERABLE_VALIDITY_RANGE_RATIO), MIN_CACHE_WARMUP_TIME);
  }

  /**
   * Checks the validity
   *
   * @param currentTime the current time
   * @param thisUpdate  the last update timestamp
   * @param nextUpdate  the next update timestamp
   * @return true if valid or false
   */
  private static boolean isValidityRange(Date currentTime, Date thisUpdate, Date nextUpdate)
  {
    long tolerableValidity = calculateTolerableVadility(thisUpdate, nextUpdate);
    return thisUpdate.getTime() - MAX_CLOCK_SKEW <= currentTime.getTime() &&
        currentTime.getTime() <= nextUpdate.getTime() + tolerableValidity;
  }

  /**
   * OCSP response cache key object
   */
  static class OcspResponseCacheKey
  {
    final byte[] nameHash;
    final byte[] keyHash;
    final BigInteger serialNumber;

    OcspResponseCacheKey(byte[] nameHash, byte[] keyHash, BigInteger serialNumber)
    {
      this.nameHash = nameHash;
      this.keyHash = keyHash;
      this.serialNumber = serialNumber;
    }

    public int hashCode()
    {
      int ret = Arrays.hashCode(this.nameHash) * 37;
      ret = ret * 10 + Arrays.hashCode(this.keyHash) * 37;
      ret = ret * 10 + this.serialNumber.hashCode();
      return ret;
    }

    public boolean equals(Object obj)
    {
      if (!(obj instanceof OcspResponseCacheKey))
      {
        return false;
      }
      OcspResponseCacheKey target = (OcspResponseCacheKey) obj;
      return Arrays.equals(this.nameHash, target.nameHash) &&
          Arrays.equals(this.keyHash, target.keyHash) &&
          this.serialNumber.equals(target.serialNumber);
    }

    public String toString()
    {
      return String.format(
          "OcspResponseCacheKey: NameHash: %s, KeyHash: %s, SerialNumber: %s",
          byteToHexString(nameHash), byteToHexString(keyHash),
          serialNumber.toString());
    }
  }

  /**
   * SHA1 Digest Calculator used in OCSP Req.
   */
  static class SHA1DigestCalculator implements DigestCalculator
  {
    private ByteArrayOutputStream bOut = new ByteArrayOutputStream();

    public AlgorithmIdentifier getAlgorithmIdentifier()
    {
      return new AlgorithmIdentifier(OIWObjectIdentifiers.idSHA1);
    }

    public OutputStream getOutputStream()
    {
      return bOut;
    }

    public byte[] getDigest()
    {
      byte[] bytes = bOut.toByteArray();
      bOut.reset();
      Digest sha1 = new SHA1Digest();
      sha1.update(bytes, 0, bytes.length);
      byte[] digest = new byte[sha1.getDigestSize()];
      sha1.doFinal(digest, 0);
      return digest;
    }
  }

}
