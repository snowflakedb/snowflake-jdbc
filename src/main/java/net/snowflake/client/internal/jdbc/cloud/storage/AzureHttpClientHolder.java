package net.snowflake.client.internal.jdbc.cloud.storage;

import com.azure.core.http.HttpClient;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import java.time.Duration;
import reactor.netty.resources.ConnectionProvider;

/**
 * Owns one Azure SDK {@link HttpClient} (and its underlying reactor-netty {@link
 * ConnectionProvider}) per JDBC session. The Azure SDK exposes no close hook on its HttpClient, so
 * the connection provider is built explicitly here and disposed at session close.
 */
final class AzureHttpClientHolder implements AutoCloseable {

  // Reactor-netty pool sizing for the Azure SDK HTTP client. These are Azure/reactor-netty
  // specific: the S3 path uses an aws-sdk Apache client (HttpUtil-driven) with a different pool
  // model, so we keep these knobs local rather than forcing a shared abstraction.
  static final int MAX_CONNECTIONS = 100;
  static final Duration MAX_IDLE_TIME = Duration.ofSeconds(30);
  static final Duration EVICT_IN_BACKGROUND_INTERVAL = Duration.ofSeconds(15);

  private final ConnectionProvider connectionProvider;
  private final HttpClient httpClient;

  private AzureHttpClientHolder(ConnectionProvider connectionProvider, HttpClient httpClient) {
    this.connectionProvider = connectionProvider;
    this.httpClient = httpClient;
  }

  static AzureHttpClientHolder create(ProxyOptions proxyOptions) {
    ConnectionProvider provider =
        ConnectionProvider.builder("snowflake-azure-blob")
            .maxConnections(MAX_CONNECTIONS)
            .maxIdleTime(MAX_IDLE_TIME)
            .evictInBackground(EVICT_IN_BACKGROUND_INTERVAL)
            .build();
    NettyAsyncHttpClientBuilder builder =
        new NettyAsyncHttpClientBuilder().connectionProvider(provider);
    if (proxyOptions != null) {
      builder.proxy(proxyOptions);
    }
    return new AzureHttpClientHolder(provider, builder.build());
  }

  HttpClient httpClient() {
    return httpClient;
  }

  @Override
  public void close() {
    connectionProvider.dispose();
  }
}
