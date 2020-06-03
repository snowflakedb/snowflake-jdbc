/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnTravisCI;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.common.core.ClientAuthnDTO;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

class MockAuthExternalBrowserHandlers
    implements SessionUtilExternalBrowser.AuthExternalBrowserHandlers
{
  @Override
  public HttpPost build(URI uri)
  {
    HttpPost httpPost = mock(HttpPost.class);
    when(httpPost.getMethod()).thenReturn("POST");
    return httpPost;
  }

  @Override
  public void openBrowser(String ssoUrl) throws SFException
  {
    // nop. Don't open browser
  }

  @Override
  public void output(String msg)
  {
    // nop. No output
  }
}

/**
 * Simulates SessionUtilExternalBrower without popping up a browser window
 */
class FakeSessionUtilExternalBrowser extends SessionUtilExternalBrowser
{
  final static String MOCK_SAML_TOKEN = "MOCK_SAML_TOKEN";
  private final ServerSocket mockServerSocket;

  public static SessionUtilExternalBrowser createInstance(
      SFLoginInput loginInput, boolean isPost)
  {
    return new FakeSessionUtilExternalBrowser(loginInput, isPost);
  }

  private FakeSessionUtilExternalBrowser(
      SFLoginInput loginInput, boolean isPost)
  {
    super(loginInput, new MockAuthExternalBrowserHandlers());
    try
    {
      this.mockServerSocket = initMockServerSocket(isPost);
    }
    catch (IOException ex)
    {
      throw new RuntimeException("Failed to initialize ServerSocket mock");
    }
  }

  /**
   * Mock ServerSocket and Socket.
   * <p>
   * Socket mock will be included in ServerSocket mock.
   *
   * @param isPost true if the response is POST request otherwise GET request
   * @return Server socket
   * @throws IOException if any IO error occurs
   */
  private static ServerSocket initMockServerSocket(boolean isPost) throws IOException
  {
    // mock client socket
    final Socket mockSocket = mock(Socket.class);
    String str;
    if (isPost)
    {
      str = String.format(
          "POST / HTTP/1.1\r\n" +
          "USER-AGENT: snowflake client\r\n" +
          "\r\n" +
          "token=%s&confirm=1",
          MOCK_SAML_TOKEN);
    }
    else
    {
      str = String.format(
          "GET /?token=%s&confirm=1 HTTP/1.1\r\n" +
          "USER-AGENT: snowflake client",
          MOCK_SAML_TOKEN);
    }
    InputStream stream = new ByteArrayInputStream(
        str.getBytes(StandardCharsets.UTF_8));
    when(mockSocket.getInputStream()).thenReturn(stream);
    when(mockSocket.getOutputStream()).thenReturn(new NullOutputStream());

    // mock server socket
    final ServerSocket mockServerSocket = mock(ServerSocket.class);
    when(mockServerSocket.getLocalPort()).thenReturn(12345);
    when(mockServerSocket.accept()).thenReturn(mockSocket);
    return mockServerSocket;
  }

  static class NullOutputStream extends OutputStream
  {
    @Override
    public void write(int b) throws IOException
    {
    }
  }

  @Override
  protected ServerSocket getServerSocket() throws SFException
  {
    return mockServerSocket;
  }

  @Override
  protected int getLocalPort(ServerSocket ssocket)
  {
    return super.getLocalPort(ssocket);
  }
}

@Ignore("powermock incompat with JDK>=9, see https://github.com/powermock/powermock/issues/901")
@RunWith(PowerMockRunner.class)
public class SessionUtilExternalBrowserTest
{
  private final static String MOCK_PROOF_KEY = "specialkey";
  private final static String MOCK_SSO_URL = "https://sso.someidp.net/";

  /**
   * Unit test for SessionUtilExternalBrowser
   *
   * @throws Throwable if any error occurs
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  @PrepareForTest({
                      net.snowflake.client.core.HttpUtil.class,
                      java.net.ServerSocket.class,
                      java.net.Socket.class
                  })
  public void testSessionUtilExternalBrowser() throws Throwable
  {
    final SFLoginInput loginInput = initMockLoginInput();

    when(HttpUtil.executeGeneralRequest(
        Mockito.any(HttpRequestBase.class),
        Mockito.anyInt(),
        Mockito.any(OCSPMode.class))).thenReturn(
        "{\"success\":\"true\",\"data\":{\"proofKey\":\"" +
        MOCK_PROOF_KEY + "\"," +
        " \"ssoUrl\":\"" + MOCK_SSO_URL + "\"}}");

    SessionUtilExternalBrowser sub =
        FakeSessionUtilExternalBrowser.createInstance(loginInput, false);
    sub.authenticate();
    assertThat("", sub.getToken(), equalTo(
        FakeSessionUtilExternalBrowser.MOCK_SAML_TOKEN));

    sub = FakeSessionUtilExternalBrowser.createInstance(loginInput, true);
    sub.authenticate();
    assertThat("", sub.getToken(), equalTo(
        FakeSessionUtilExternalBrowser.MOCK_SAML_TOKEN));
  }

  /**
   * Unit test for SessionUtilExternalBrowser (fail)
   *
   * @throws Throwable if any error occurs
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  @PrepareForTest({
                      net.snowflake.client.core.HttpUtil.class,
                      java.net.ServerSocket.class,
                      java.net.Socket.class
                  })
  public void testSessionUtilExternalBrowserFail() throws Throwable
  {
    final SFLoginInput loginInput = initMockLoginInput();

    when(HttpUtil.executeGeneralRequest(
        Mockito.any(HttpRequestBase.class),
        Mockito.anyInt(),
        Mockito.any(OCSPMode.class))).thenReturn(
        "{\"success\":\"false\",\"code\":\"123456\",\"message\":\"errormes\"}");

    SessionUtilExternalBrowser sub =
        FakeSessionUtilExternalBrowser.createInstance(loginInput, false);
    try
    {
      sub.authenticate();
      fail("should have failed with an exception.");
    }
    catch (SnowflakeSQLException ex)
    {
      assertThat("Error is expected", ex.getErrorCode(),
                 equalTo(123456));
    }
  }

  /**
   * Mock HttpUtil and SFLoginInput
   *
   * @return a mock object for SFLoginInput
   */
  private SFLoginInput initMockLoginInput()
  {
    // mock HttpUtil
    mockStatic(HttpUtil.class);

    // mock SFLoginInput
    SFLoginInput loginInput = mock(SFLoginInput.class);
    when(loginInput.getServerUrl()).thenReturn(
        "https://testaccount.snowflakecomputing.com/");
    when(loginInput.getAuthenticator()).thenReturn(
        ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER.name());
    when(loginInput.getAccountName()).thenReturn("testaccount");
    when(loginInput.getUserName()).thenReturn("testuser");
    return loginInput;
  }

}
