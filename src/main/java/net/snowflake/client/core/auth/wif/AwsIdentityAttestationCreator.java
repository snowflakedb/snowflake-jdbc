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
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class AwsIdentityAttestationCreator implements WorkloadIdentityAttestationCreator {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(AwsIdentityAttestationCreator.class);
  public static final String API_VERSION = "2011-06-15";
  public static final String GET_CALLER_IDENTITY_ACTION = "GetCallerIdentity";

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
    AWSCredentials awsCredentials;

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
    String region = attestationService.getAWSRegion();
    if (region == null) {
      throw new SFException(ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "No AWS region was found");
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
