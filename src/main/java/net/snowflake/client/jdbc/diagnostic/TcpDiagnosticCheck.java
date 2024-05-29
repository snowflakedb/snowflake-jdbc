package net.snowflake.client.jdbc.diagnostic;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketTimeoutException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class TcpDiagnosticCheck extends DiagnosticCheck {

  private static final SFLogger logger = SFLoggerFactory.getLogger(TcpDiagnosticCheck.class);

  TcpDiagnosticCheck(ProxyConfig proxyConfig) {
    super("TCP Connection Test", proxyConfig);
  }

  public void run(SnowflakeEndpoint snowflakeEndpoint) {
    super.run(snowflakeEndpoint);
    String hostname = snowflakeEndpoint.getHost();
    int connectTimeoutMillis = 60000;
    int port = snowflakeEndpoint.getPort();
    Proxy proxy = proxyConf.getProxy(snowflakeEndpoint);
    try (Socket socket = new Socket(proxy)) {
      socket.bind(null);
      logger.debug(
          "Establishing TCP connection: {} -> {}:{}",
          socket.getLocalSocketAddress(),
          snowflakeEndpoint.getHost(),
          snowflakeEndpoint.getPort());
      socket.connect(new InetSocketAddress(hostname, port), connectTimeoutMillis);
      logger.debug(
          "Established a TCP connection successfully: {} -> {}",
          socket.getLocalSocketAddress(),
          socket.getRemoteSocketAddress());
    } catch (SocketTimeoutException e) {
      logger.error(
          "Could not establish TCP connection within timeout of {} ms", connectTimeoutMillis);
      logger.error(e.getLocalizedMessage(), e);
    } catch (IOException e) {
      logger.error(
          "Error connecting to host " + hostname + ":" + port + ":" + e.getLocalizedMessage(), e);
    }
  }
}
