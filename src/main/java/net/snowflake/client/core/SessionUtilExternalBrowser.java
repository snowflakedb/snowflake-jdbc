/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.awt.Desktop;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * SAML 2.0 Compliant service/application federated authentication
 * 1. Query GS to obtain IDP SSO url
 * 2. Listen a localhost port to accept Saml response
 * 3. Open a browser in the backend so that the user can type IdP username
 * and password.
 * 4. Return token and proof key to the GS to gain access.
 */
public class SessionUtilExternalBrowser
{
  static final SFLogger logger = SFLoggerFactory.getLogger(
      SessionUtilExternalBrowser.class);

  public interface AuthExternalBrowserHandlers
  {
    // build a HTTP post object
    HttpPost build(URI uri);

    // open a browser
    void openBrowser(String ssoUrl) throws SFException;

    // output
    void output(String msg);
  }

  static class DefaultAuthExternalBrowserHandlers implements AuthExternalBrowserHandlers
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
  private AuthExternalBrowserHandlers handlers;
  private static final String PREFIX_GET = "GET ";
  private static final String PREFIX_POST = "POST ";
  private static final String PREFIX_USER_AGENT = "USER-AGENT: ";
  private static Charset UTF8_CHARSET;

  static
  {
    UTF8_CHARSET = Charset.forName("UTF-8");
  }

  public static SessionUtilExternalBrowser createInstance(SessionUtil.LoginInput loginInput)
  {
    return new SessionUtilExternalBrowser(
        loginInput, new DefaultAuthExternalBrowserHandlers());
  }

  public SessionUtilExternalBrowser(SessionUtil.LoginInput loginInput, AuthExternalBrowserHandlers handlers)
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
  protected ServerSocket getServerSocket() throws SFException
  {
    try
    {
      return new ServerSocket(
          0, // free port
          0, // default number of connections
          InetAddress.getByName("localhost"));
    }
    catch (IOException ex)
    {
      throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
    }
  }

  /**
   * Get a port listening
   *
   * @param ssocket server socket
   * @return port number
   */
  protected int getLocalPort(ServerSocket ssocket)
  {
    return ssocket.getLocalPort();
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
      data.put(ClientAuthnParameter.CLIENT_APP_ID.name(), loginInput.getAppId());
      data.put(ClientAuthnParameter.CLIENT_APP_VERSION.name(),
          loginInput.getAppVersion());

      authnData.setData(data);
      String json = mapper.writeValueAsString(authnData);

      // attach the login info json body to the post request
      StringEntity input = new StringEntity(json, Charset.forName("UTF-8"));
      input.setContentType("application/json");
      postRequest.setEntity(input);

      postRequest.addHeader("accept", "application/json");

      String theString = HttpUtil.executeRequest(postRequest,
          loginInput.getLoginTimeout(), 0, null);

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

  /**
   * Authenticate
   *
   * @throws SFException           if any error occurs
   * @throws SnowflakeSQLException if any error occurs
   */
  void authenticate() throws SFException, SnowflakeSQLException
  {
    ServerSocket ssocket = this.getServerSocket();
    try
    {
      // main procedure
      int port = this.getLocalPort(ssocket);
      logger.debug("Listening localhost:{}", port);
      String ssoUrl = getSSOUrl(port);
      this.handlers.output(
          "Initiating login request with your identity provider. A " +
              "browser window should have opened for you to complete the " +
              "login. If you can't see it, check existing browser windows, " +
              "or your OS settings. Press CTRL+C to abort and try again...");
      this.handlers.openBrowser(ssoUrl);

      receiveSamlToken(ssocket);
    }
    catch (IOException ex)
    {
      throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
    }
    finally
    {
      try
      {
        ssocket.close();
      }
      catch (IOException ex)
      {
        throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
      }
    }
  }

  /**
   * Receives SAML token from Snowflake via web browser
   *
   * @param ssocket server socket
   * @throws IOException if any IO error occurs
   * @throws SFException if a HTTP request from browser is invalid
   */
  private void receiveSamlToken(ServerSocket ssocket) throws IOException, SFException
  {
    Socket socket = ssocket.accept(); // start accepting the request
    try
    {
      BufferedReader in = new BufferedReader(
          new InputStreamReader(socket.getInputStream(), UTF8_CHARSET));
      char[] buf = new char[16384];
      int strLen = in.read(buf);
      String[] rets = new String(buf, 0, strLen).split("\r\n");
      String targetLine = null;
      String userAgent = null;
      boolean isPost = false;
      for (String line : rets)
      {
        if (line.length() > PREFIX_GET.length() &&
            line.substring(0, PREFIX_GET.length()).equalsIgnoreCase(PREFIX_GET))
        {
          targetLine = line;
        }
        else if (line.length() > PREFIX_POST.length() &&
            line.substring(0, PREFIX_POST.length()).equalsIgnoreCase(PREFIX_POST))
        {
          targetLine = rets[rets.length-1];
          isPost = true;
        }
        else if (line.length() > PREFIX_USER_AGENT.length() &&
            line.substring(0, PREFIX_USER_AGENT.length()).equalsIgnoreCase(PREFIX_USER_AGENT))
        {
          userAgent = line;
        }
      }
      if (targetLine == null)
      {
        throw new SFException(ErrorCode.NETWORK_ERROR,
            "Invalid HTTP request. No token is given from the browser.");
      }
      if (userAgent != null)
      {
        logger.debug("{}", userAgent);
      }

      String parameters = isPost ?
          extractTokenFromPostRequest(targetLine) :
          extractTokenFromGetRequest(targetLine);

      try
      {
        this.token = null;
        URI inputParameter = new URI(parameters);
        for (NameValuePair urlParam: URLEncodedUtils.parse(
            inputParameter, UTF8_CHARSET)) {
          if ("token".equals(urlParam.getName())) {
            this.token = urlParam.getValue();
            break;
          }
        }
        if (this.token == null)
        {
          throw new SFException(ErrorCode.NETWORK_ERROR,
              String.format(
                  "Invalid HTTP request. No token is given from the browser: %s",
                  targetLine));
        }
      }
      catch(URISyntaxException ex)
      {
        throw new SFException(ErrorCode.NETWORK_ERROR,
            String.format(
                "Invalid HTTP request. No token is given from the browser: %s",
                targetLine));
      }

      returnToBrowser(socket);
    }
    finally
    {
      socket.close();
    }
  }

  private String extractTokenFromPostRequest(String targetLine)
  {
    return "/?" + targetLine;
  }

  private String extractTokenFromGetRequest(String targetLine) throws SFException
  {
    String[] elems = targetLine.split("\\s");
    if (elems.length != 3 ||
        !elems[0].toLowerCase(Locale.US).equalsIgnoreCase("GET") ||
        !elems[2].startsWith("HTTP/1."))
    {
      throw new SFException(ErrorCode.NETWORK_ERROR,
          String.format(
              "Invalid HTTP request. No token is given from the browser: %s",
              targetLine));
    }
    return elems[1];
  }

  /**
   * Output the message to the browser
   *
   * @param socket client socket
   * @throws IOException if any IO error occurs
   */
  private void returnToBrowser(Socket socket) throws IOException
  {
    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

    String responseText =
        "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>" +
            "<title>SAML Response for Snowflake</title></head>" +
            "<body>Your identity was confirmed and propagated to " +
            "Snowflake JDBC driver. You can close this window now and go back " +
            "where you started from.</body></html>";
    String[] content = new String[]{
        "HTTP/1.0 200 OK",
        "Content-Type: text/html",
        String.format("Content-Length: %s", responseText.length()),
        "",
        responseText
    };
    for (int i = 0; i < content.length; ++i)
    {
      if (i > 0)
      {
        out.print("\r\n");
      }
      out.print(content[i]);
    }
    out.flush();
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
}
