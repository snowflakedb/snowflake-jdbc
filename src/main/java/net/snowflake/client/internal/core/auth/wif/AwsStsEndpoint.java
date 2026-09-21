package net.snowflake.client.internal.core.auth.wif;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.endpoints.StsEndpointParams;
import software.amazon.awssdk.services.sts.endpoints.StsEndpointProvider;

/**
 * Resolved STS endpoint used by AWS Workload Identity Federation.
 *
 * <p>When {@code workloadIdentityHost} is set it is the user-supplied override; otherwise it is the
 * regional default from the AWS SDK endpoint rules (FIPS, dualstack and the legacy global endpoint
 * pinned off). Resolution of the default is a pure evaluation of those rules: no network calls and
 * no credentials.
 */
public final class AwsStsEndpoint {
  private final String authority;
  private final String baseUrl;
  private final boolean overridden;

  AwsStsEndpoint(String authority, String baseUrl, boolean overridden) {
    this.authority = authority;
    this.baseUrl = baseUrl;
    this.overridden = overridden;
  }

  String getAuthority() {
    return authority;
  }

  String getBaseUrl() {
    return baseUrl;
  }

  boolean isOverridden() {
    return overridden;
  }

  URI getBaseUri() {
    return URI.create(baseUrl);
  }

  static AwsStsEndpoint resolve(String workloadIdentityHost, String region) throws SFException {
    if (!SnowflakeUtil.isNullOrEmpty(workloadIdentityHost)) {
      return parseWorkloadIdentityHost(workloadIdentityHost);
    }
    return defaultStsEndpoint(region);
  }

  /**
   * Normalizes user input: accepts a bare host or a full URL, defaults the scheme to https, strips
   * trailing slashes, and rejects user info, a query, a fragment, a non-http(s) scheme, or any path
   * other than empty or {@code /}.
   */
  public static AwsStsEndpoint parseWorkloadIdentityHost(String host) throws SFException {
    if (host == null) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "workloadIdentityHost is empty");
    }
    host = host.trim();
    if (host.isEmpty()) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "workloadIdentityHost is empty");
    }

    if (!host.contains("://")) {
      host = "https://" + host;
    }

    URI uri;
    try {
      uri = new URI(host);
    } catch (URISyntaxException e) {
      throw new SFException(
          e,
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "workloadIdentityHost \"" + host + "\" is malformed: " + e.getMessage());
    }

    String scheme = uri.getScheme();
    // http is accepted as well as https: the driver already allows http for the Snowflake
    // connection itself, and a local mock STS used with role chaining or outbound token needs it.
    if (!"https".equalsIgnoreCase(scheme) && !"http".equalsIgnoreCase(scheme)) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "workloadIdentityHost \""
              + host
              + "\" must use https or http, got scheme \""
              + scheme
              + "\"");
    }
    if (uri.getHost() == null || uri.getHost().isEmpty()) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "workloadIdentityHost \"" + host + "\" does not contain a hostname");
    }
    if (uri.getUserInfo() != null || uri.getRawQuery() != null || uri.getFragment() != null) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "workloadIdentityHost \""
              + host
              + "\" must not contain user info, a query or a fragment");
    }
    String path = uri.getPath() == null ? "" : trimTrailingSlashes(uri.getPath());
    if (!path.isEmpty() && !"/".equals(path)) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "workloadIdentityHost \"" + host + "\" must not contain a path");
    }

    URI base;
    try {
      base = new URI(uri.getScheme(), uri.getAuthority(), null, null, null);
    } catch (URISyntaxException e) {
      throw new SFException(
          e,
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "workloadIdentityHost \"" + host + "\" is malformed: " + e.getMessage());
    }

    String baseUrl = trimTrailingSlashes(base.toString());
    return new AwsStsEndpoint(uri.getAuthority(), baseUrl, true);
  }

  /**
   * Resolves the regional STS endpoint using the AWS SDK's own endpoint rules. The DNS suffix is
   * partition-specific and not derivable from the region name — the ISO partitions and the European
   * Sovereign Cloud do not use amazonaws.com at all — so formatting the hostname here would be
   * wrong for every partition the SDK already knows about.
   */
  static AwsStsEndpoint defaultStsEndpoint(String region) throws SFException {
    if (SnowflakeUtil.isNullOrEmpty(region)) {
      throw new SFException(ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "No AWS region was found");
    }
    Endpoint resolved;
    try {
      resolved =
          StsEndpointProvider.defaultProvider()
              .resolveEndpoint(
                  StsEndpointParams.builder()
                      .region(Region.of(region))
                      .useFips(false)
                      .useDualStack(false)
                      .useGlobalEndpoint(false)
                      .build())
              .join();
    } catch (CompletionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new SFException(
          cause,
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "could not resolve an STS endpoint for region \"" + region + "\": " + cause.getMessage());
    }

    URI uri = resolved.url();
    if (uri == null || uri.getHost() == null || uri.getHost().isEmpty()) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "resolved STS endpoint for region \"" + region + "\" has no host: \"" + uri + "\"");
    }

    URI base;
    try {
      base = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, null);
    } catch (URISyntaxException e) {
      throw new SFException(
          e,
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "resolved STS endpoint for region \"" + region + "\" is malformed: " + e.getMessage());
    }
    return new AwsStsEndpoint(uri.getAuthority(), trimTrailingSlashes(base.toString()), false);
  }

  private static String trimTrailingSlashes(String value) {
    int end = value.length();
    while (end > 0 && value.charAt(end - 1) == '/') {
      end--;
    }
    return value.substring(0, end);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AwsStsEndpoint)) {
      return false;
    }
    AwsStsEndpoint that = (AwsStsEndpoint) o;
    return overridden == that.overridden
        && Objects.equals(authority, that.authority)
        && Objects.equals(baseUrl, that.baseUrl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(authority, baseUrl, overridden);
  }
}
