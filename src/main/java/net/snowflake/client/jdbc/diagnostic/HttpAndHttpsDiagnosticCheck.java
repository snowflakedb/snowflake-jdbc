package net.snowflake.client.jdbc.diagnostic;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

class HttpAndHttpsDiagnosticCheck extends DiagnosticCheck {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(HttpAndHttpsDiagnosticCheck.class);
  private final String HTTP_SCHEMA = "http://";
  private final String HTTPS_SCHEMA = "https://";

  HttpAndHttpsDiagnosticCheck(ProxyConfig proxyConfig) {
    super("HTTP/HTTPS Connection Test", proxyConfig);
  }

  @Override
  protected void doCheck(SnowflakeEndpoint snowflakeEndpoint) {
    // We have to replace underscores with hyphens because the JDK doesn't allow underscores in the
    // hostname
    String hostname = snowflakeEndpoint.getHost().replace('_', '-');
    try {
      Proxy proxy = this.proxyConf.getProxy(snowflakeEndpoint);
      StringBuilder sb = new StringBuilder();
      String urlString =
          (snowflakeEndpoint.isSslEnabled()) ? HTTPS_SCHEMA + hostname : HTTP_SCHEMA + hostname;
      URL url = new URL(urlString);
      HttpURLConnection con =
          (snowflakeEndpoint.isSslEnabled())
              ? (HttpsURLConnection) url.openConnection(proxy)
              : (HttpURLConnection) url.openConnection(proxy);
      logger.info("Response from server: {} {}", con.getResponseCode(), con.getResponseMessage());
      sb.append("Headers:\n");

      Map<String, List<String>> headerFields = con.getHeaderFields();
      for (Map.Entry<String, List<String>> header : headerFields.entrySet()) {
        sb.append(header.getKey()).append(": ").append(header.getValue()).append("\n");
      }

      logger.info(sb.toString());

    } catch (MalformedURLException e) {
      logger.error(
          "The URL format is incorrect, please check your allowlist JSON file for errors.", e);
    } catch (IOException e) {
      logger.error("Could not send an HTTP/HTTPS request to host " + hostname, e);
    }
  }
}
