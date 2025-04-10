package net.snowflake.client.core.auth.wif;

import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.http.HttpMethodName;
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

  private final AwsAttestationService attestationService;
  private AWSCredentials awsCredentials;
  private String region;
  private String arn;

  public AwsIdentityAttestationCreator(AwsAttestationService attestationService) {
    this.attestationService = attestationService;
  }

  @Override
  public WorkloadIdentityAttestation createAttestation() {
    logger.debug("Creating AWS identity attestation...");
    this.awsCredentials = attestationService.getAWSCredentials();
    if (this.awsCredentials == null) {
      logger.debug("No AWS credentials were found.");
      return null;
    }
    this.region = attestationService.getAWSRegion();
    if (this.region == null) {
      logger.debug("No AWS region was found.");
      return null;
    }
    this.arn = attestationService.getArn();
    if (this.arn == null) {
      logger.debug("No Caller Identity was found.");
      return null;
    }

    String stsHostname = String.format("sts.%s.amazonaws.com", region);
    Request<Void> request = createStsRequest(stsHostname);
    attestationService.signRequestWithSigV4(request, awsCredentials);

    String credential = createBase64EncodedRequestCredential(request);
    return new WorkloadIdentityAttestation(
        WorkloadIdentityProviderType.AWS, credential, Collections.singletonMap("arn", arn));
  }

  private Request<Void> createStsRequest(String hostname) {
    Request<Void> request = new DefaultRequest<>("sts");
    request.setHttpMethod(HttpMethodName.POST);
    request.setEndpoint(
        URI.create(
            String.format("https://%s/?Action=GetCallerIdentity&Version=2011-06-15", hostname)));
    request.addHeader("Host", hostname);
    request.addHeader(
        WorkloadIdentityUtil.SNOWFLAKE_AUDIENCE_HEADER_NAME,
        WorkloadIdentityUtil.SNOWFLAKE_AUDIENCE);
    request.addHeader("X-Amz-Security-Token", "<TOKEN>");
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

  AWSCredentials getAwsCredentials() {
    return awsCredentials;
  }

  String getRegion() {
    return region;
  }

  String getArn() {
    return arn;
  }
}
