/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.ClientAuthnDTO;
import net.snowflake.common.core.ClientAuthnParameter;
import net.snowflake.common.core.SqlState;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;

import java.awt.*;
import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SAML 2.0 Compliant service/application federated authentication
 * 1. Query GS to obtain IDP SSO url
 * 2. Listen a localhost port to accept Saml response
 * 3. Open a browser in the backend so that the user can type IdP username
 * and password.
 * 4. Return token and proofkey to the GS to gain access.
 */
class SessionUtilExternalBrowser
{
  static final SFLogger logger = SFLoggerFactory.getLogger(
      SessionUtilExternalBrowser.class);

  /**
   * Web Server state class
   * <p>
   * Two variables are included. forceToStop is set to true if the
   * Web Server receives the SAML token from GS such that the web server
   * will be stopped.
   * token is set to the value from GS.
   */
  class WebServerState
  {
    public volatile boolean forceToStop = false;
    public volatile String token = "";
  }

  final WebServerState webServerState = new WebServerState();

  public interface AuthExternalBrowserHandlers
  {
    // build a HTTP post object
    HttpPost build(URI uri);

    // open a browser
    void openBrowser(String ssoUrl) throws SFException;

    // output
    void output(String msg);
  }

  class DefaultAuthExternalBrowserHandlers implements AuthExternalBrowserHandlers
  {
    @Override
    public HttpPost build(URI uri)
    {
      return new HttpPost(uri);
    }

    @Override
    public void openBrowser(String ssoUrl) throws SFException
    {
      try
      {
        // start web browser
        if (Desktop.isDesktopSupported())
        {
          URI uri = new URI(ssoUrl);
          Desktop.getDesktop().browse(uri);
        }
        else
        {
          Runtime runtime = Runtime.getRuntime();
          Constants.OS os = Constants.getOS();
          if (os == Constants.OS.MAC)
          {
            runtime.exec("open " + ssoUrl);
          }
          else
          {
            // linux?
            runtime.exec("xdg-open " + ssoUrl);
          }
        }
      }
      catch (URISyntaxException | IOException ex)
      {
        throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
      }
    }

    @Override
    public void output(String msg)
    {
      System.out.println(msg);
    }

  }

  private final ObjectMapper mapper;

  private final SessionUtil.LoginInput loginInput;

  String token;
  private String proofKey;
  private final AuthExternalBrowserHandlers handlers;

  SessionUtilExternalBrowser(SessionUtil.LoginInput loginInput)
  {
    this.mapper = new ObjectMapper();
    this.loginInput = loginInput;
    this.handlers = new DefaultAuthExternalBrowserHandlers();
  }

  SessionUtilExternalBrowser(
      SessionUtil.LoginInput loginInput, AuthExternalBrowserHandlers handlers)
  {
    this.mapper = new ObjectMapper();
    this.loginInput = loginInput;
    this.handlers = handlers;
  }

  /**
   * Gets a free port on localhost
   *
   * @return port number
   * @throws SFException raised if an error occurs.
   */
  private int getPort() throws SFException
  {
    try
    {
      ServerSocket s = new ServerSocket(
          0, // free port
          50, // default number of connections
          InetAddress.getByName("localhost"));
      int port = s.getLocalPort();
      s.close();
      return port;
    }
    catch (IOException ex)
    {
      throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
    }
  }

  /**
   * Gets SSO URL and proof key
   *
   * @return SSO URL.
   * @throws SFException           if Snowflake error occurs
   * @throws SnowflakeSQLException if Snowflake SQL error occurs
   */
  private String getSSOUrl(int port) throws SFException, SnowflakeSQLException
  {
    try
    {
      String serverUrl = loginInput.getServerUrl();
      String authenticator = loginInput.getAuthenticator();

      URIBuilder fedUriBuilder = new URIBuilder(serverUrl);
      fedUriBuilder.setPath(SessionUtil.SF_PATH_AUTHENTICATOR_REQUEST);
      URI fedUrlUri = fedUriBuilder.build();

      HttpPost postRequest = this.handlers.build(fedUrlUri);

      ClientAuthnDTO authnData = new ClientAuthnDTO();
      Map<String, Object> data = new HashMap<>();

      data.put(ClientAuthnParameter.AUTHENTICATOR.name(), authenticator);
      data.put(ClientAuthnParameter.ACCOUNT_NAME.name(),
          loginInput.getAccountName());
      data.put(ClientAuthnParameter.LOGIN_NAME.name(),
          loginInput.getUserName());
      data.put(ClientAuthnParameter.BROWSER_MODE_REDIRECT_PORT.name(),
          Integer.toString(port));

      authnData.setData(data);
      String json = mapper.writeValueAsString(authnData);

      // attach the login info json body to the post request
      StringEntity input = new StringEntity(json, Charset.forName("UTF-8"));
      input.setContentType("application/json");
      postRequest.setEntity(input);

      postRequest.addHeader("accept", "application/json");

      String theString = HttpUtil.executeRequest(postRequest,
          loginInput.getHttpClient(), loginInput.getLoginTimeout(),
          0, null);

      logger.debug("authenticator-request response: {}", theString);

      // general method, same as with data binding
      JsonNode jsonNode = mapper.readTree(theString);

      // check the success field first
      if (!jsonNode.path("success").asBoolean())
      {
        logger.debug("response = {}", theString);
        String errorCode = jsonNode.path("code").asText();
        throw new SnowflakeSQLException(
            SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
            new Integer(errorCode),
            jsonNode.path("message").asText());
      }

      JsonNode dataNode = jsonNode.path("data");

      // session token is in the data field of the returned json response
      this.proofKey = dataNode.path("proofKey").asText();
      return dataNode.path("ssoUrl").asText();
    }
    catch (IOException | URISyntaxException ex)
    {
      throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
    }
  }

  void authenticate() throws SFException, SnowflakeSQLException
  {
    final int RETRY = 10;
    HttpServer server = null;
    int port = 0;

    for (int i = 0; i < RETRY; ++i)
    {
      port = this.getPort(); // get a free port
      try
      {
        // create a HTTP server instance
        server = HttpServer.create(new InetSocketAddress(
            InetAddress.getByName("localhost"), port), 0);
        break;
      }
      catch (BindException ex)
      {
        logger.debug("Address already in use. port=%s, Retry...", port);
      }
      catch (IOException ex)
      {
        throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
      }
    }
    if (server == null)
    {
      throw new SFException(ErrorCode.NETWORK_ERROR,
          "Failed to start a web server that accepts a SAML token from Snowflake.");
    }

    HttpHandler handler = new WebServerHandler(this.webServerState);
    Thread th = new Thread(new WebServer(server, handler));
    th.start(); // run a web server in a separate thread

    try
    {
      // main procedure
      String ssoUrl = getSSOUrl(port);
      this.handlers.output(
          "Initiating login request with your identity provider. A " +
              "browser window should have opened for you to complete the " +
              "login. If you can't see it, check existing browser windows, " +
              "or your OS settings. Press CTRL+C to abort and try again...");
      this.handlers.openBrowser(ssoUrl);

      while (!this.webServerState.forceToStop)
      {
        try
        {
          logger.debug("waiting for GS to come back with SAML token.");
          Thread.sleep(1000);
        }
        catch (InterruptedException ex)
        {
          throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
        }
      }
    }
    finally
    {
      server.stop(1); // stop web server.
      logger.debug("stopped the web server.");
      this.token = this.webServerState.token;
      try
      {
        logger.debug("stopping the web server thread.");
        th.join(2000);
      }
      catch (InterruptedException ex)
      {
        throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
      }
    }
  }

  /**
   * Returns encoded SAML token
   *
   * @return SAML token
   */
  String getToken()
  {
    return this.token;
  }

  /**
   * Returns proofkey provided in the first roundtrip with GS and
   * back to GS in the last login-request authentication.
   *
   * @return proofkey
   */
  String getProofKey()
  {
    return this.proofKey;
  }

  class WebServerHandler implements HttpHandler
  {
    private final WebServerState webServerState;

    WebServerHandler(WebServerState webServerState)
    {
      this.webServerState = webServerState;
    }

    @Override
    public void handle(HttpExchange t) throws IOException
    {
      List<NameValuePair> params = URLEncodedUtils.parse(t.getRequestURI(),
          Charset.forName("utf-8"));
      for (NameValuePair param : params)
      {
        if ("token".equals(param.getName()))
        {
          this.webServerState.token = param.getValue();
          break;
        }
      }
      this.webServerState.forceToStop = true;
      Headers headers = t.getResponseHeaders();
      headers.set("Content-Type", "text/html");
      String responseText =
          "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>" +
              "<title>SAML Response for Snowflake</title></head>" +
              "<body>Your identity was confirmed and propagated to " +
              "Snowflake JDBC driver. You can close this window now and go back " +
              "where you started from.</body></html>";
      OutputStream out = t.getResponseBody();

      t.sendResponseHeaders(200, responseText.length());
      out.write(responseText.getBytes());
      out.close();
    }

    public String getToken()
    {
      return webServerState.token;
    }
  }

  class WebServer implements Runnable
  {
    private final HttpServer server;
    private final HttpHandler handler;
    private String token;

    WebServer(HttpServer server, HttpHandler handler)
    {
      this.server = server;
      this.handler = handler;
      this.token = "";
    }

    @Override
    public void run()
    {
      server.createContext("/", handler);
      server.setExecutor(null);
      server.start();
    }

    public String getToken()
    {
      return this.token;
    }
  }
}
