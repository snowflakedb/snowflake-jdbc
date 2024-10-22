package net.snowflake.client.jdbc.diagnostic;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketTimeoutException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

class TcpDiagnosticCheck extends DiagnosticCheck {

  private static final SFLogger logger = SFLoggerFactory.getLogger(TcpDiagnosticCheck.class);

  TcpDiagnosticCheck(ProxyConfig proxyConfig) {
    super("TCP Connection Test", proxyConfig);
  }

  protected void doCheck(SnowflakeEndpoint snowflakeEndpoint) {
    String hostname = snowflakeEndpoint.getHost();
    int connectTimeoutMillis = 60000;
    int port = snowflakeEndpoint.getPort();
    Proxy proxy = proxyConf.getProxy(snowflakeEndpoint);
    try (Socket socket = new Socket(proxy)) {
      socket.bind(null);
      logger.info(
          "Establishing TCP connection: {} -> {}:{}",
          socket.getLocalSocketAddress(),
          snowflakeEndpoint.getHost(),
          snowflakeEndpoint.getPort());
      socket.connect(new InetSocketAddress(hostname, port), connectTimeoutMillis);
      logger.info(
          "Established a TCP connection successfully: {} -> {}",
          socket.getLocalSocketAddress(),
          socket.getRemoteSocketAddress());
    } catch (SocketTimeoutException e) {
      logger.error(
          "Could not establish TCP connection within timeout of " + connectTimeoutMillis + "ms", e);
    } catch (IOException e) {
      logger.error("Error connecting to host " + hostname + ":" + port, e);
    } catch (Exception e) {
      logger.error("Unexpected error occurred when connecting to host " + hostname + ":" + port, e);
    }
  }
}
