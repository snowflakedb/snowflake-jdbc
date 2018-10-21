/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AnyOf.anyOf;

public class SFTrustManagerIT
{
  private static final String[] TARGET_HOSTS = {
      "sfcsupport.snowflakecomputing.com",
      "sfcsupport.us-east-1.snowflakecomputing.com",
      "sfcsupport.eu-central-1.snowflakecomputing.com",
      "sfc-dev1-regression.s3.amazonaws.com",
      "sfc-ds2-customer-stage.s3.amazonaws.com",
      "snowflake.okta.com",
      "sfcdev1.blob.core.windows.net"
  };

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  /**
   * OCSP tests for the Snowflake and AWS S3 HTTPS connections.
   * <p>
   * Whatever the default method is used.
   */
  @Test
  public void testOcsp() throws Throwable
  {
    for (String host : TARGET_HOSTS)
    {
      HttpClient client = HttpUtil.buildHttpClient(
          false, // NOT insecure mode
          null, // default OCSP response cache file
          true // use OCSP response cache server
      );
      accessHost(host, client);
    }
  }

  /**
   * OCSP tests for the Snowflake and AWS S3 HTTPS connections using the
   * file cache.
   * <p>
   * Specifying an non-existing file will force to fetch OCSP response.
   */
  @Test
  public void testOcspWithFileCache() throws Throwable
  {
    File ocspCacheFile = tmpFolder.newFile();
    for (String host : TARGET_HOSTS)
    {
      HttpClient client = HttpUtil.buildHttpClient(
          false, // NOT insecure mode
          ocspCacheFile, // a temp OCSP response cache file
          false // NOT use OCSP response cache server
      );
      accessHost(host, client);
    }
  }

  /**
   * OCSP tests for the Snowflake and AWS S3 HTTPS connections using the
   * server cache.
   */
  @Test
  public void testOcspWithServerCache() throws Throwable
  {
    File ocspCacheFile = tmpFolder.newFile();
    for (String host : TARGET_HOSTS)
    {
      HttpClient client = HttpUtil.buildHttpClient(
          false, // NOT insecure mode
          ocspCacheFile, // a temp OCSP response cache file
          true // use OCSP response cache server
      );
      accessHost(host, client);
    }
  }

  /**
   * OCSP tests for the Snowflake and AWS S3 HTTPS connections using the
   * server cache.
   */
  @Test
  public void testInvalidCacheFile() throws Throwable
  {
    // a file under never exists.
    File ocspCacheFile = new File("NEVER_EXISTS", "NEVER_EXISTS");
    String host = TARGET_HOSTS[0];
    HttpClient client = HttpUtil.buildHttpClient(
        false, // NOT insecure mode
        ocspCacheFile, // a temp OCSP response cache file
        true // use OCSP response cache server
    );
    accessHost(host, client);
  }

  private static void accessHost(String host, HttpClient client) throws IOException
  {
    final int maxRetry = 10;
    int statusCode = -1;
    for (int retry = 0; retry < maxRetry; ++retry)
    {
      HttpGet httpRequest = new HttpGet(String.format("https://%s:443/", host));
      HttpResponse response = client.execute(httpRequest);
      statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != 503 && statusCode != 504)
      {
        break;
      }
      try
      {
        Thread.sleep(1000L);
      }
      catch (InterruptedException ex)
      {
      }
    }
    assertThat(String.format("response code for %s", host),
        statusCode,
        anyOf(equalTo(200), equalTo(403), equalTo(400)));
  }

  /**
   * Revoked certificate test.
   */
  @Test
  public void testRevokedCertificate() throws Throwable
  {
    File ocspCacheFile = tmpFolder.newFile();
    List<X509Certificate> certList = getX509CertificatesFromFile(
        "revoked_certs.pem");
    SFTrustManager sft = new SFTrustManager(
        ocspCacheFile,  // a temp OCSP response cache file
        true);
    try
    {
      sft.validateRevocationStatus(certList.toArray(new X509Certificate[0]));
      fail();
    }
    catch (CertificateEncodingException ex)
    {
      assertThat(ex.getMessage(), containsString("has been revoked"));
    }
  }

  /**
   * Read certificates from a file.
   *
   * @param filename file name under resources directory
   * @return an array of X509Certificate
   * @throws Throwable raise if any error occurs
   */
  private List<X509Certificate> getX509CertificatesFromFile(String filename) throws Throwable
  {
    CertificateFactory fact = CertificateFactory.getInstance("X.509");
    List<X509Certificate> certList = new ArrayList<>();
    for (Certificate cert : fact.generateCertificates(
        getFile(filename)))
    {
      certList.add((X509Certificate) cert);
    }
    return certList;
  }

  private InputStream getFile(String fileName) throws Throwable
  {
    ClassLoader classLoader = getClass().getClassLoader();
    URL url = classLoader.getResource(fileName);
    return url.openStream();
  }
}