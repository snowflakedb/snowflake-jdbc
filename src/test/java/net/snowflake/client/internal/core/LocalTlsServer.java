package net.snowflake.client.internal.core;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

/**
 * A loopback TLS server pinned to an explicit set of protocol versions, for testing that the driver
 * refuses to negotiate outside its configured range.
 *
 * <p>Presents a generated {@link LoopbackCertificate}, so clients that verify hostnames can
 * complete a handshake against it.
 *
 * <p>Tests assert on {@link #awaitNegotiatedProtocol()} -- what the server actually negotiated --
 * rather than on the client's outcome, which distinguishes "rejected at protocol negotiation" from
 * "rejected later for an unrelated reason". A client-side exception alone cannot.
 */
final class LocalTlsServer implements AutoCloseable {

  /** Sentinel queued when a connection failed before a session was established. */
  static final String HANDSHAKE_FAILED = "HANDSHAKE_FAILED";

  private final SSLServerSocket serverSocket;
  private final BlockingQueue<String> outcomes = new LinkedBlockingQueue<>();
  private volatile boolean running = true;

  LocalTlsServer(String... enabledProtocols) throws Exception {
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(keyManagers(), null, null);
    // Bind 127.0.0.1 explicitly rather than InetAddress.getLoopbackAddress(), which may resolve to
    // ::1 -- the generated certificate's SAN covers the IPv4 loopback, so a client verifying the
    // hostname against ::1 would fail for a reason unrelated to what these tests assert.
    serverSocket =
        (SSLServerSocket)
            context
                .getServerSocketFactory()
                .createServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
    serverSocket.setEnabledProtocols(enabledProtocols);

    Thread acceptor = new Thread(this::acceptLoop, "local-tls-server");
    acceptor.setDaemon(true);
    acceptor.start();
  }

  int getPort() {
    return serverSocket.getLocalPort();
  }

  String getHost() {
    return serverSocket.getInetAddress().getHostAddress();
  }

  /**
   * @return the protocol negotiated for the next connection, or {@link #HANDSHAKE_FAILED}; null if
   *     no connection arrived in time
   */
  String awaitNegotiatedProtocol() throws InterruptedException {
    return outcomes.poll(15, TimeUnit.SECONDS);
  }

  private void acceptLoop() {
    while (running) {
      try (SSLSocket socket = (SSLSocket) serverSocket.accept()) {
        String outcome;
        try {
          socket.startHandshake();
          outcome = socket.getSession().getProtocol();
        } catch (IOException handshakeFailure) {
          outcome = HANDSHAKE_FAILED;
        }
        // record exactly one outcome per connection: a read failure after a successful handshake
        // must not also enqueue HANDSHAKE_FAILED, or a later poll would see a stale sentinel
        outcomes.put(outcome);
        try {
          // drain a little so the client is not left waiting on an empty stream
          socket.getInputStream().read();
        } catch (IOException ignored) {
          // client already gone; the outcome is already recorded
        }
      } catch (Exception stopping) {
        // socket closed during shutdown, or a client vanished; nothing to record
      }
    }
  }

  private static javax.net.ssl.KeyManager[] keyManagers() throws Exception {
    KeyManagerFactory factory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    factory.init(LoopbackCertificate.newKeyStore(), LoopbackCertificate.PASSWORD);
    return factory.getKeyManagers();
  }

  @Override
  public void close() {
    running = false;
    try {
      serverSocket.close();
    } catch (IOException ignored) {
      // shutting down
    }
  }
}
