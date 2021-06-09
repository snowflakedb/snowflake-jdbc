package net.snowflake.client.core;

import com.google.common.base.Strings;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;

public class HttpClientSettingsKey {

    private OCSPMode ocspMode;
    private boolean useProxy;
    private String proxyHost = "";
    private int proxyPort = 0;
    private String nonProxyHosts = "";
    private String proxyUser = "";
    private String proxyPassword = "";

    public HttpClientSettingsKey(OCSPMode mode, String host, int port, String nonProxyHosts, String user, String password) {
        this.useProxy = true;
        this.ocspMode = mode;
        this.proxyHost = host;
        this.proxyPort = port;
        this.nonProxyHosts = nonProxyHosts;
        this.proxyUser = user;
        this.proxyPassword = password;
    }

    public HttpClientSettingsKey(OCSPMode mode)
    {
        this.useProxy = false;
        this.ocspMode = mode;
    }

    @Override
    public boolean equals (final Object O) {
        if (O instanceof HttpClientSettingsKey)
        {
            if (((HttpClientSettingsKey) O).ocspMode.getValue() == this.ocspMode.getValue())
            {
                if (!((HttpClientSettingsKey) O).useProxy)
                {
                    return true;
                }
                else if (((HttpClientSettingsKey) O).proxyHost.trim().equalsIgnoreCase(this.proxyHost)) {
                    if (((HttpClientSettingsKey) O).proxyPort == this.proxyPort){
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.ocspMode.getValue() + (this.proxyHost + this.proxyPort).hashCode();
    }

    public OCSPMode getOcspMode()
    {
        return this.ocspMode;
    }

    public boolean usesProxy()
    {
        return this.useProxy;
    }

    public HttpHost getProxy()
    {
        HttpHost proxy = new HttpHost(this.proxyHost, this.proxyPort);
        return proxy;
    }

    public SnowflakeProxyRoutePlanner getProxyRoutePlanner()
    {
        SnowflakeProxyRoutePlanner sdkProxyRoutePlanner = new SnowflakeProxyRoutePlanner(this.proxyHost, this.proxyPort, this.nonProxyHosts);
        sdkProxyRoutePlanner.setNonProxyHosts(nonProxyHosts);
        return sdkProxyRoutePlanner;
    }

  public CredentialsProvider getProxyCredentialsProvider()
  {
      if (!Strings.isNullOrEmpty(this.proxyUser) && !Strings.isNullOrEmpty(this.proxyPassword)) {
          Credentials credentials = new UsernamePasswordCredentials(this.proxyUser, this.proxyPassword);
          AuthScope authScope = new AuthScope(this.proxyHost, this.proxyPort);
          CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials(authScope, credentials);
          return credentialsProvider;
      }
      return null;
  }
}
