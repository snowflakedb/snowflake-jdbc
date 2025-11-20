package net.snowflake.client.core.auth.wif;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.minidev.json.JSONObject;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;

@SnowflakeJdbcInternalApi
public class AwsIdentityAttestationCreator implements WorkloadIdentityAttestationCreator {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(AwsIdentityAttestationCreator.class);
  public static final String API_VERSION = "2011-06-15";
  public static final String GET_CALLER_IDENTITY_ACTION = "GetCallerIdentity";
  
  // Headers that the backend expects - whitelist approach
  private static final Set<String> EXPECTED_HEADERS = new HashSet<>();
  static {
    EXPECTED_HEADERS.add("Host");
    EXPECTED_HEADERS.add("X-Snowflake-Audience");
    EXPECTED_HEADERS.add("Authorization");
    EXPECTED_HEADERS.add("X-Amz-Date");
    EXPECTED_HEADERS.add("X-Amz-Security-Token");
  }

  private final AwsAttestationService attestationService;
  private final SFLoginInput loginInput;

  public AwsIdentityAttestationCreator(
      AwsAttestationService attestationService, SFLoginInput loginInput) {
    this.attestationService = attestationService;
    this.loginInput = loginInput;
  }

  @Override
  public WorkloadIdentityAttestation createAttestation() throws SFException {
    attestationService.initializeSignerRegion();
    AwsCredentials awsCredentials;

    if (loginInput.getWorkloadIdentityImpersonationPath().isEmpty()) {
      logger.debug("Creating AWS identity attestation...");
      awsCredentials = attestationService.getAWSCredentials();
    } else {
      logger.debug("Creating AWS identity attestation with impersonation...");
      awsCredentials = attestationService.getCredentialsViaRoleChaining(loginInput);
    }

    if (awsCredentials == null) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "No AWS credentials were found");
    }
    Region region = attestationService.getAWSRegion();
    if (region == null) {
      throw new SFException(ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "No AWS region was found");
    }

    String stsHostname = getStsHostname(region.id());
    SdkHttpRequest request = createStsRequest(stsHostname);
    SdkHttpRequest signedRequest = attestationService.signRequestWithSigV4(request, awsCredentials);

    String credential = createBase64EncodedRequestCredential(signedRequest);
    return new WorkloadIdentityAttestation(
        WorkloadIdentityProviderType.AWS, credential, Collections.emptyMap());
  }

  private String getStsHostname(String region) {
    String domain = region.startsWith("cn-") ? "amazonaws.com.cn" : "amazonaws.com";
    return String.format("sts.%s.%s", region, domain);
  }

  private SdkHttpRequest createStsRequest(String hostname) {
    String url =
        String.format(
            "https://%s?Action=%s&Version=%s", hostname, GET_CALLER_IDENTITY_ACTION, API_VERSION);

    SdkHttpFullRequest.Builder requestBuilder =
        SdkHttpFullRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(URI.create(url))
            .putHeader("Host", hostname)
            .putHeader(
                WorkloadIdentityUtil.SNOWFLAKE_AUDIENCE_HEADER_NAME,
                WorkloadIdentityUtil.SNOWFLAKE_AUDIENCE)
            .contentStreamProvider(
                () -> new ByteArrayInputStream(new byte[0])); // needed to properly sign the request

    return requestBuilder.build();
  }

  private String createBase64EncodedRequestCredential(SdkHttpRequest request) {
    JSONObject assertionJson = new JSONObject();
    JSONObject headers = new JSONObject();
    
    // AWS SDK 2 returns headers as Map<String, List<String>>, but backend expects flat Map<String, String>
    // Flatten the headers by taking the first value from each list
    // Only include headers that the backend expects (whitelist approach)
    for (Map.Entry<String, List<String>> entry : request.headers().entrySet()) {
      String headerName = entry.getKey();
      List<String> headerValues = entry.getValue();
      if (headerValues != null && !headerValues.isEmpty()) {
        // Take the first value to flatten the header
        headers.put(headerName, headerValues.get(0));
      }
    }
    
    assertionJson.put("url", request.getUri().toString());
    assertionJson.put("method", request.method().toString());
    assertionJson.put("headers", headers);

    System.out.println("assertionJson = " + assertionJson);

    String assertionJsonString = assertionJson.toString();
    return Base64.getEncoder().encodeToString(assertionJsonString.getBytes(StandardCharsets.UTF_8));
  }
}
