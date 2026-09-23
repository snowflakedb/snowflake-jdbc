package net.snowflake.client.internal.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.awt.Desktop;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.auth.ClientAuthnDTO;
import net.snowflake.client.internal.core.auth.ClientAuthnParameter;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;

/**
 * SAML 2.0 Compliant service/application federated authentication 1. Query GS to obtain IDP SSO url
 * 2. Listen a localhost port to accept Saml response 3. Open a browser in the backend so that the
 * user can type IdP username and password. 4. Return token and proof key to the GS to gain access.
 */
public class SessionUtilExternalBrowser {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SessionUtilExternalBrowser.class);

  public interface AuthExternalBrowserHandlers {
    // build a HTTP post object
    HttpPost build(URI uri);

    // open a browser
    void openBrowser(String ssoUrl) throws SFException;

    // output
    void output(String msg);
  }

  public static class DefaultAuthExternalBrowserHandlers implements AuthExternalBrowserHandlers {
    @Override
    public HttpPost build(URI uri) {
      return new HttpPost(uri);
    }

    @Override
    public void openBrowser(String ssoUrl) throws SFException {
      if (!URLUtil.isValidURL(ssoUrl)) {
        throw new SFException(ErrorCode.INVALID_CONNECTION_URL, "Invalid SSOUrl found - " + ssoUrl);
      }
      try {
        // start web browser
        Runtime runtime = Runtime.getRuntime();
        Constants.OS os = Constants.getOS();
        if (Desktop.isDesktopSupported()
            && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
          Desktop.getDesktop().browse(new URI(ssoUrl));
        } else if (os == Constants.OS.MAC) {
          runtime.exec(new String[] {"open", ssoUrl});
        } else if (os == Constants.OS.WINDOWS) {
          runtime.exec(new String[] {"rundll32", "url.dll,FileProtocolHandler", ssoUrl});
        } else {
          runtime.exec(new String[] {"xdg-open", ssoUrl});
        }
      } catch (URISyntaxException | IOException ex) {
        throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
      }
    }

    @Override
    public void output(String msg) {
      System.out.println(msg);
    }
  }

  private final ObjectMapper mapper;

  private final SFLoginInput loginInput;

  String token;
  private boolean consentCacheIdToken;
  private String proofKey;
  private AuthExternalBrowserHandlers handlers;

  // Upper bound on a single localhost callback request we will buffer. The largest legitimate
  // request (SSO token plus headers) is a few KB; this only guards against a misbehaving local
  // client that advertises a huge Content-Length or never sends the end-of-headers marker.
  private static final int MAX_REQUEST_LENGTH = 1 << 20; // 1 MiB
  private static Charset UTF8_CHARSET;

  static {
    UTF8_CHARSET = Charset.forName("UTF-8");
  }

  public static SessionUtilExternalBrowser createInstance(SFLoginInput loginInput) {
    return new SessionUtilExternalBrowser(loginInput, new DefaultAuthExternalBrowserHandlers());
  }

  public SessionUtilExternalBrowser(SFLoginInput loginInput, AuthExternalBrowserHandlers handlers) {
    this.mapper = ObjectMapperFactory.getObjectMapper();
    this.loginInput = loginInput;
    this.handlers = handlers;
    this.consentCacheIdToken = true; // true by default
  }

  /**
   * Gets a free port on localhost
   *
   * @return port number
   * @throws SFException raised if an error occurs.
   */
  protected ServerSocket getServerSocket() throws SFException {
    try {
      return new ServerSocket(
          0, // free port
          0, // default number of connections
          InetAddress.getByName("localhost"));
    } catch (IOException ex) {
      throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
    }
  }

  /**
   * Get a port listening
   *
   * @param ssocket server socket
   * @return port number
   */
  protected int getLocalPort(ServerSocket ssocket) {
    return ssocket.getLocalPort();
  }

  /**
   * Gets SSO URL and proof key
   *
   * @return SSO URL.
   * @throws SFException if Snowflake error occurs
   * @throws SnowflakeSQLException if Snowflake SQL error occurs
   */
  private String getSSOUrl(int port) throws SFException, SnowflakeSQLException {
    try {
      String serverUrl = loginInput.getServerUrl();
      String authenticator = loginInput.getAuthenticator();

      URIBuilder fedUriBuilder = new URIBuilder(serverUrl);
      fedUriBuilder.setPath(SessionUtil.SF_PATH_AUTHENTICATOR_REQUEST);
      URI fedUrlUri = fedUriBuilder.build();

      HttpPost postRequest = this.handlers.build(fedUrlUri);

      Map<String, Object> data = new HashMap<>();

      data.put(ClientAuthnParameter.AUTHENTICATOR.name(), authenticator);
      data.put(ClientAuthnParameter.ACCOUNT_NAME.name(), loginInput.getAccountName());
      data.put(ClientAuthnParameter.LOGIN_NAME.name(), loginInput.getUserName());
      data.put(ClientAuthnParameter.BROWSER_MODE_REDIRECT_PORT.name(), Integer.toString(port));
      data.put(ClientAuthnParameter.CLIENT_APP_ID.name(), loginInput.getAppId());
      data.put(ClientAuthnParameter.CLIENT_APP_VERSION.name(), loginInput.getAppVersion());

      ClientAuthnDTO authnData = new ClientAuthnDTO(data, null);
      String json = mapper.writeValueAsString(authnData);

      // attach the login info json body to the post request
      StringEntity input = new StringEntity(json, StandardCharsets.UTF_8);
      input.setContentType("application/json");
      postRequest.setEntity(input);

      postRequest.addHeader("accept", "application/json");

      String theString =
          HttpUtil.executeGeneralRequestWithContext(
                  postRequest,
                  loginInput.getLoginTimeout(),
                  loginInput.getAuthTimeout(),
                  loginInput.getSocketTimeoutInMillis(),
                  0,
                  0,
                  loginInput.getHttpClientSettingsKey(),
                  null)
              .getResponseBody();

      logger.debug("Authenticator-request response: {}", theString);

      // general method, same as with data binding
      JsonNode jsonNode = mapper.readTree(theString);

      // check the success field first
      if (!jsonNode.path("success").asBoolean()) {
        logger.debug("Response: {}", theString);
        String errorCode = jsonNode.path("code").asText();
        throw new SnowflakeSQLException(
            SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
            Integer.valueOf(errorCode),
            jsonNode.path("message").asText());
      }

      JsonNode dataNode = jsonNode.path("data");

      // session token is in the data field of the returned json response
      this.proofKey = dataNode.path("proofKey").asText();
      return dataNode.path("ssoUrl").asText();
    } catch (IOException | URISyntaxException ex) {
      throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
    }
  }

  private String getConsoleLoginUrl(int port) throws SFException {
    try {
      proofKey = generateProofKey();
      String serverUrl = loginInput.getServerUrl();

      URIBuilder consoleLoginUriBuilder = new URIBuilder(serverUrl);
      consoleLoginUriBuilder.setPath(SessionUtil.SF_PATH_CONSOLE_LOGIN_REQUEST);
      consoleLoginUriBuilder.addParameter("login_name", loginInput.getUserName());
      consoleLoginUriBuilder.addParameter("browser_mode_redirect_port", Integer.toString(port));
      consoleLoginUriBuilder.addParameter("proof_key", proofKey);

      String consoleLoginUrl = consoleLoginUriBuilder.build().toURL().toString();

      logger.debug("Console login url: {}", consoleLoginUrl);

      return consoleLoginUrl;
    } catch (Exception ex) {
      throw new SFException(ex, ErrorCode.INTERNAL_ERROR, ex.getMessage());
    }
  }

  private String generateProofKey() {
    SecureRandom secureRandom = new SecureRandom();
    byte[] randomness = new byte[32];
    secureRandom.nextBytes(randomness);
    return Base64.getEncoder().encodeToString(randomness);
  }

  private int getBrowserResponseTimeout() {
    return (int) loginInput.getBrowserResponseTimeout().toMillis();
  }

  /**
   * Authenticate
   *
   * @throws SFException if any error occurs
   * @throws SnowflakeSQLException if any error occurs
   */
  void authenticate() throws SFException, SnowflakeSQLException {
    ServerSocket ssocket = this.getServerSocket();
    try {
      ssocket.setSoTimeout(getBrowserResponseTimeout());
      // main procedure
      int port = this.getLocalPort(ssocket);
      logger.debug("Listening localhost: {}", port);

      if (loginInput.getDisableConsoleLogin()) {
        // Access GS to get SSO URL
        String ssoUrl = getSSOUrl(port);
        this.handlers.output(
            "Initiating login request with your identity provider. A "
                + "browser window should have opened for you to complete the "
                + "login. If you can't see it, check existing browser windows, "
                + "or your OS settings. Press CTRL+C to abort and try again...");
        this.handlers.openBrowser(ssoUrl);
      } else {
        // Multiple SAML way to do authentication via console login
        String consoleLoginUrl = getConsoleLoginUrl(port);
        this.handlers.output(
            "Initiating login request with your identity provider(s). A "
                + "browser window should have opened for you to complete the "
                + "login. If you can't see it, check existing browser windows, "
                + "or your OS settings. Press CTRL+C to abort and try again...");
        this.handlers.openBrowser(consoleLoginUrl);
      }

      while (true) {
        try (Socket socket = ssocket.accept()) {
          // Bound reads so a client that connects then stalls mid-request cannot hang the server.
          socket.setSoTimeout(getBrowserResponseTimeout());
          BufferedReader in =
              new BufferedReader(new InputStreamReader(socket.getInputStream(), UTF8_CHARSET));
          String request = readRequest(in);
          if (request.isEmpty()) {
            // Browser preconnect/speculative socket closed without sending data. See SNOW-3704231.
            logger.debug("Received empty request on localhost callback socket. Ignoring.");
            continue;
          }
          if (processOptions(request, socket)) {
            continue;
          }
          String method = getRequestMethod(request);
          String requestOrigin = extractHeader(request, "Origin");
          if (!isCallbackOriginAllowed(method, requestOrigin)) {
            logger.debug("Ignoring localhost callback whose Origin does not match the server URL.");
            continue;
          }
          processSamlToken(request, socket, getResponseOrigin(requestOrigin));
          break;
        }
      }
    } catch (SocketTimeoutException e) {
      throw new SFException(
          e,
          ErrorCode.NETWORK_ERROR,
          "External browser authentication failed within timeout of "
              + getBrowserResponseTimeout()
              + " milliseconds");
    } catch (IOException ex) {
      throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
    } finally {
      try {
        ssocket.close();
      } catch (IOException ex) {
        throw new SFException(ex, ErrorCode.NETWORK_ERROR, ex.getMessage());
      }
    }
  }

  /**
   * Reads a full HTTP request from the localhost callback socket, reassembling fragments until the
   * LF or CRLF end-of-headers marker plus the full {@code Content-Length} body, or EOF. Returns an
   * empty string when the peer closed without sending data (e.g. a browser preconnect socket).
   */
  private String readRequest(BufferedReader in) throws IOException {
    char[] buf = new char[16384];
    StringBuilder request = new StringBuilder();
    int strLen;
    int bodyStart = -1;
    int contentLength = 0;
    while ((strLen = in.read(buf)) >= 0) {
      request.append(buf, 0, strLen);
      if (request.length() > MAX_REQUEST_LENGTH) {
        throw new IOException(
            "Localhost callback request exceeded " + MAX_REQUEST_LENGTH + " chars; aborting read.");
      }
      if (bodyStart < 0) {
        // Resolve the header terminator and Content-Length once, not on every read.
        int headerEnd = findHeaderEnd(request);
        if (headerEnd >= 0) {
          bodyStart = headerEnd + headerTerminatorLength(request, headerEnd);
          contentLength = getContentLength(request.substring(0, headerEnd));
        }
      }
      // No body expected (GET / preflight / no Content-Length) breaks at the terminator so we
      // don't block awaiting an absent body. Otherwise gate on the cheap char count first (UTF-8
      // byte length is always >= char count) and only then confirm the exact byte count, since
      // Content-Length is a byte count.
      if (bodyStart >= 0
          && (contentLength == 0
              || (request.length() - bodyStart >= contentLength
                  && bodyByteLength(request, bodyStart) >= contentLength))) {
        break;
      }
    }
    return request.toString();
  }

  /** UTF-8 byte length of the request body accumulated so far. */
  private int bodyByteLength(StringBuilder request, int bodyStart) {
    return request.substring(bodyStart).getBytes(UTF8_CHARSET).length;
  }

  /**
   * Parses the {@code Content-Length} header (case-insensitive), returning 0 when it is absent or
   * unparseable.
   */
  private int getContentLength(String headers) {
    for (String line : headers.split("\\r?\\n")) {
      int colon = line.indexOf(':');
      if (colon > 0 && line.substring(0, colon).trim().equalsIgnoreCase("Content-Length")) {
        try {
          return Integer.parseInt(line.substring(colon + 1).trim());
        } catch (NumberFormatException e) {
          return 0;
        }
      }
    }
    return 0;
  }

  private boolean processOptions(String request, Socket socket) throws IOException {
    if (!"OPTIONS".equalsIgnoreCase(getRequestMethod(request))) {
      return false;
    }

    String userAgent = extractHeader(request, "User-Agent");
    if (userAgent != null) {
      logger.debug("USER-AGENT: {}", userAgent);
    }
    String requestOrigin = extractHeader(request, "Origin");
    String requestedMethod = extractHeader(request, "Access-Control-Request-Method");
    String requestedHeaders = extractHeader(request, "Access-Control-Request-Headers");
    if (requestOrigin != null
        && originMatchesServer(requestOrigin, loginInput.getServerUrl())
        && "POST".equalsIgnoreCase(requestedMethod)
        && requestedHeadersAllowed(requestedHeaders)) {
      returnToBrowserForOptions(requestOrigin, socket);
    } else {
      logger.debug("Ignoring localhost CORS preflight that does not match callback requirements.");
    }
    return true;
  }

  private void returnToBrowserForOptions(String requestOrigin, Socket socket) throws IOException {
    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
    SimpleDateFormat fmt = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss");
    fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    String[] content = {
      "HTTP/1.1 200 OK",
      String.format("Date: %s", fmt.format(new Date()) + " GMT"),
      "Access-Control-Allow-Methods: POST",
      "Access-Control-Allow-Headers: Content-Type",
      "Access-Control-Max-Age: 86400",
      String.format("Access-Control-Allow-Origin: %s", requestOrigin),
      "",
      ""
    };
    for (int i = 0; i < content.length; ++i) {
      if (i > 0) {
        out.print("\r\n");
      }
      out.print(content[i]);
    }
    out.flush();
  }

  /**
   * Receives SAML token from Snowflake via web browser
   *
   * @param socket socket
   * @throws IOException if any IO error occurs
   * @throws SFException if a HTTP request from browser is invalid
   */
  private void processSamlToken(String request, Socket socket, String responseOrigin)
      throws IOException, SFException {
    String targetLine = null;
    String method = getRequestMethod(request);
    boolean isPost = "POST".equalsIgnoreCase(method);
    if ("GET".equalsIgnoreCase(method)) {
      targetLine = getRequestLine(request);
    } else if (isPost) {
      targetLine = getRequestBody(request);
    }
    String userAgent = extractHeader(request, "User-Agent");
    if (targetLine == null) {
      throw new SFException(
          ErrorCode.NETWORK_ERROR, "Invalid HTTP request. No token is given from the browser.");
    }
    if (userAgent != null) {
      logger.debug("USER-AGENT: {}", userAgent);
    }

    try {
      // attempt to get JSON response
      extractJsonTokenFromPostRequest(targetLine);
    } catch (IOException ex) {
      String parameters =
          isPost ? extractTokenFromPostRequest(targetLine) : extractTokenFromGetRequest(targetLine);
      try {
        URI inputParameter = new URI(parameters);
        for (NameValuePair urlParam : URLEncodedUtils.parse(inputParameter, UTF8_CHARSET)) {
          if ("token".equals(urlParam.getName())) {
            this.token = urlParam.getValue();
            break;
          }
        }
      } catch (URISyntaxException ex0) {
        throw new SFException(
            ErrorCode.NETWORK_ERROR,
            String.format(
                "Invalid HTTP request. No token is given from the browser. %s, err: %s",
                targetLine, ex0));
      }
    }
    if (this.token == null) {
      throw new SFException(
          ErrorCode.NETWORK_ERROR,
          String.format(
              "Invalid HTTP request. No token is given from the browser: %s", targetLine));
    }

    returnToBrowser(socket, responseOrigin);
  }

  private void extractJsonTokenFromPostRequest(String targetLine) throws IOException {
    JsonNode jsonNode = mapper.readTree(targetLine);
    this.token = jsonNode.get("token").asText();
    this.consentCacheIdToken = jsonNode.get("consent").asBoolean();
  }

  private String extractTokenFromPostRequest(String targetLine) {
    return "/?" + targetLine;
  }

  private String extractTokenFromGetRequest(String targetLine) throws SFException {
    String[] elems = targetLine.split("\\s");
    if (elems.length != 3
        || !elems[0].toLowerCase(Locale.US).equalsIgnoreCase("GET")
        || !elems[2].startsWith("HTTP/1.")) {
      throw new SFException(
          ErrorCode.NETWORK_ERROR,
          String.format(
              "Invalid HTTP request. No token is given from the browser: %s", targetLine));
    }
    return elems[1];
  }

  /**
   * Output the message to the browser
   *
   * @param socket client socket
   * @throws IOException if any IO error occurs
   */
  private void returnToBrowser(Socket socket, String responseOrigin) throws IOException {
    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

    List<String> content = new ArrayList<>();
    content.add("HTTP/1.0 200 OK");
    content.add("Content-Type: text/html");
    String responseText;
    if (responseOrigin != null) {
      content.add(String.format("Access-Control-Allow-Origin: %s", responseOrigin));
      content.add("Vary: Accept-Encoding, Origin");
      Map<String, Object> data = new HashMap<>();
      data.put("consent", this.consentCacheIdToken);
      responseText = mapper.writeValueAsString(data);
    } else {
      responseText =
          "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>"
              + "<title>SAML Response for Snowflake</title></head>"
              + "<body>Your identity was confirmed and propagated to "
              + "Snowflake JDBC driver. You can close this window now and go back "
              + "where you started from.</body></html>";
    }
    content.add(String.format("Content-Length: %s", responseText.length()));
    content.add("");
    content.add(responseText);

    for (int i = 0; i < content.size(); ++i) {
      if (i > 0) {
        out.print("\r\n");
      }
      out.print(content.get(i));
    }
    out.flush();
  }

  private boolean isCallbackOriginAllowed(String method, String requestOrigin) {
    if ("GET".equalsIgnoreCase(method)
        && (requestOrigin == null || "null".equalsIgnoreCase(requestOrigin))) {
      return true;
    }
    return requestOrigin != null && originMatchesServer(requestOrigin, loginInput.getServerUrl());
  }

  private String getResponseOrigin(String requestOrigin) {
    return requestOrigin == null || "null".equalsIgnoreCase(requestOrigin) ? null : requestOrigin;
  }

  private static boolean requestedHeadersAllowed(String requestedHeaders) {
    if (requestedHeaders == null) {
      return true;
    }
    for (String header : requestedHeaders.split(",")) {
      String trimmed = header.trim();
      if (!trimmed.isEmpty() && !"Content-Type".equalsIgnoreCase(trimmed)) {
        return false;
      }
    }
    return true;
  }

  private static boolean originMatchesServer(String requestOrigin, String serverUrl) {
    try {
      URI origin = new URI(requestOrigin);
      URI server = new URI(serverUrl);
      return isHttpScheme(origin.getScheme())
          && origin.getScheme().equalsIgnoreCase(server.getScheme())
          && origin.getHost() != null
          && origin.getHost().equalsIgnoreCase(server.getHost())
          && effectivePort(origin) == effectivePort(server);
    } catch (URISyntaxException ex) {
      return false;
    }
  }

  private static boolean isHttpScheme(String scheme) {
    return "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);
  }

  private static int effectivePort(URI uri) {
    if (uri.getPort() >= 0) {
      return uri.getPort();
    }
    return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
  }

  private static String extractHeader(String request, String headerName) {
    String headers = getHeaderSection(request);
    for (String line : headers.split("\\r?\\n")) {
      int colon = line.indexOf(':');
      if (colon > 0 && headerName.equalsIgnoreCase(line.substring(0, colon).trim())) {
        return line.substring(colon + 1).trim();
      }
    }
    return null;
  }

  private static String getRequestMethod(String request) {
    String requestLine = getRequestLine(request);
    int separator = requestLine.indexOf(' ');
    return separator < 0 ? requestLine : requestLine.substring(0, separator);
  }

  private static String getRequestLine(String request) {
    int lineEnd = request.indexOf('\n');
    String requestLine = lineEnd < 0 ? request : request.substring(0, lineEnd);
    return requestLine.endsWith("\r")
        ? requestLine.substring(0, requestLine.length() - 1)
        : requestLine;
  }

  private static String getHeaderSection(String request) {
    int headerEnd = findHeaderEnd(request);
    return headerEnd < 0 ? request : request.substring(0, headerEnd);
  }

  private static String getRequestBody(String request) {
    int headerEnd = findHeaderEnd(request);
    return headerEnd < 0
        ? ""
        : request.substring(headerEnd + headerTerminatorLength(request, headerEnd));
  }

  private static int findHeaderEnd(CharSequence request) {
    String value = request.toString();
    int crlfEnd = value.indexOf("\r\n\r\n");
    int lfEnd = value.indexOf("\n\n");
    if (crlfEnd < 0) {
      return lfEnd;
    }
    if (lfEnd < 0) {
      return crlfEnd;
    }
    return Math.min(crlfEnd, lfEnd);
  }

  private static int headerTerminatorLength(CharSequence request, int headerEnd) {
    return request.length() >= headerEnd + 4
            && request.charAt(headerEnd) == '\r'
            && request.charAt(headerEnd + 1) == '\n'
            && request.charAt(headerEnd + 2) == '\r'
            && request.charAt(headerEnd + 3) == '\n'
        ? 4
        : 2;
  }

  /**
   * Returns encoded SAML token
   *
   * @return SAML token
   */
  String getToken() {
    return this.token;
  }

  /**
   * Returns proofkey provided in the first roundtrip with GS and back to GS in the last
   * login-request authentication.
   *
   * @return proofkey
   */
  String getProofKey() {
    return this.proofKey;
  }

  /**
   * True if the user consented to cache id token
   *
   * @return true or false
   */
  boolean isConsentCacheIdToken() {
    return this.consentCacheIdToken;
  }
}
