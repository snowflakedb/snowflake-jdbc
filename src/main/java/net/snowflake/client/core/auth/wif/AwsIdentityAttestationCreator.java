package net.snowflake.client.core.auth.wif;

import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.http.HttpMethodName;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import net.minidev.json.JSONObject;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class AwsIdentityAttestationCreator implements WorkloadIdentityAttestationCreator {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(AwsIdentityAttestationCreator.class);
  public static final String API_VERSION = "2011-06-15";
  public static final String GET_CALLER_IDENTITY_ACTION = "GetCallerIdentity";

  private final AwsAttestationService attestationService;

  public AwsIdentityAttestationCreator(AwsAttestationService attestationService) {
    this.attestationService = attestationService;
  }

  @Override
  public WorkloadIdentityAttestation createAttestation() {
    logger.debug("Creating AWS identity attestation...");
    AWSCredentials awsCredentials = attestationService.getAWSCredentials();
    if (awsCredentials == null) {
      logger.debug("No AWS credentials were found.");
      return null;
    }
    String region = attestationService.getAWSRegion();
    if (region == null) {
      logger.debug("No AWS region was found.");
      return null;
    }

    String stsHostname = getStsHostname(region);
    Request<Void> request = createStsRequest(stsHostname);
    attestationService.signRequestWithSigV4(request, awsCredentials);

    String credential = createBase64EncodedRequestCredential(request);
    return new WorkloadIdentityAttestation(
        WorkloadIdentityProviderType.AWS, credential, Collections.emptyMap());
  }

  private String getStsHostname(String region) {
    String domain = region.startsWith("cn-") ? "amazonaws.com.cn" : "amazonaws.com";
    return String.format("sts.%s.%s", region, domain);
  }

  private Request<Void> createStsRequest(String hostname) {
    Request<Void> request = new DefaultRequest<>("sts");
    request.setHttpMethod(HttpMethodName.POST);
    request.setEndpoint(
        URI.create(
            String.format(
                "https://%s?Action=%s&Version=%s",
                hostname, GET_CALLER_IDENTITY_ACTION, API_VERSION)));
    request.addParameter("Action", GET_CALLER_IDENTITY_ACTION);
    request.addParameter("Version", API_VERSION);
    request.setContent(
        new ByteArrayInputStream(new byte[0])); // needed to properly sign the request
    request.addHeader("Host", hostname);
    request.addHeader(
        WorkloadIdentityUtil.SNOWFLAKE_AUDIENCE_HEADER_NAME,
        WorkloadIdentityUtil.SNOWFLAKE_AUDIENCE);
    return request;
  }

  private String createBase64EncodedRequestCredential(Request<Void> request) {
    JSONObject assertionJson = new JSONObject();
    JSONObject headers = new JSONObject();
    headers.putAll(request.getHeaders());
    assertionJson.put("url", request.getEndpoint().toString());
    assertionJson.put("method", request.getHttpMethod().toString());
    assertionJson.put("headers", headers);

    String assertionJsonString = assertionJson.toString();
    return Base64.getEncoder().encodeToString(assertionJsonString.getBytes(StandardCharsets.UTF_8));
  }
}
