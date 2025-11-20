package net.snowflake.client.core.auth.wif;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.EnvironmentVariables;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.AwsSessionCredentialsIdentity;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

@SnowflakeJdbcInternalApi
public class AwsAttestationService {

  public static final SFLogger logger = SFLoggerFactory.getLogger(AwsAttestationService.class);

  private static boolean regionInitialized = false;
  private static Region region;

  private final AwsV4HttpSigner aws4Signer;

  public AwsAttestationService() {
    aws4Signer = AwsV4HttpSigner.create();
  }

  void initializeSignerRegion() {
    getAWSRegion(); // Ensure region is initialized
  }

  public AwsCredentials getAWSCredentials() {
    try {
      AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
      return credentialsProvider.resolveCredentials();
    } catch (Exception e) {
      logger.debug("Failed to retrieve AWS credentials: {}", e.getMessage());
      return null;
    }
  }

  Region getAWSRegion() {
    if (!regionInitialized) {
      logger.debug("Getting AWS region from environment variable");
      String envRegion = SnowflakeUtil.systemGetEnv(EnvironmentVariables.AWS_REGION.getName());
      if (envRegion != null) {
        region = Region.of(envRegion);
      } else {
        logger.debug("Getting AWS region from default region provider chain");
        region = DefaultAwsRegionProviderChain.builder().build().getRegion();
      }
      regionInitialized = true;
    }
    return region;
  }

  SdkHttpRequest signRequestWithSigV4(
      SdkHttpRequest signableRequest, AwsCredentials awsCredentials) {
    AwsCredentialsIdentity credentialsIdentity;
    if (awsCredentials instanceof AwsSessionCredentials) {
      AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) awsCredentials;

      // Create AwsSessionCredentialsIdentity that properly includes the session token
      credentialsIdentity =
          AwsSessionCredentialsIdentity.create(
              sessionCredentials.accessKeyId(),
              sessionCredentials.secretAccessKey(),
              sessionCredentials.sessionToken());
    } else {
      // For basic credentials, use AwsCredentialsIdentity
      credentialsIdentity =
          AwsCredentialsIdentity.create(
              awsCredentials.accessKeyId(), awsCredentials.secretAccessKey());
    }
    
    System.out.println(
        "signableRequest = " + signableRequest + ", awsCredentials = " + awsCredentials);
    SignRequest<AwsCredentialsIdentity> signRequest =
        SignRequest.builder(credentialsIdentity)
            .request(signableRequest)
            .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, "sts")
            .putProperty(AwsV4HttpSigner.REGION_NAME, getAWSRegion().toString())
            .putProperty(AwsV4HttpSigner.PAYLOAD_SIGNING_ENABLED, false)
            .build();

    System.out.println(
        "signRequest = " + signableRequest);

//    return aws4Signer.sign(signRequest).request();
    return customSignRequestWithSigV4(signableRequest, awsCredentials);
  }
  
  // Alternative custom signer implementation that excludes x-amz-content-sha256
  SdkHttpRequest customSignRequestWithSigV4(
      SdkHttpRequest signableRequest, AwsCredentials awsCredentials) {
    
    System.out.println(
        "Using custom signer - signableRequest = " + signableRequest + ", awsCredentials = " + awsCredentials);
    
    // Use custom signer that excludes x-amz-content-sha256 header
    return customSignRequest(signableRequest, awsCredentials);
  }
  
  private SdkHttpRequest customSignRequest(SdkHttpRequest request, AwsCredentials credentials) {
    try {
      String accessKey = credentials.accessKeyId();
      String secretKey = credentials.secretAccessKey();
      String sessionToken = null;
      
      if (credentials instanceof AwsSessionCredentials) {
        sessionToken = ((AwsSessionCredentials) credentials).sessionToken();
      }
      
      // Get current timestamp
      Instant now = Instant.now();
      String amzDate = now.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'"));
      String dateStamp = now.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
      
      // Add required headers (excluding x-amz-content-sha256)
      SdkHttpRequest.Builder requestBuilder = request.toBuilder()
          .putHeader("X-Amz-Date", amzDate);
      
      if (sessionToken != null) {
        requestBuilder.putHeader("X-Amz-Security-Token", sessionToken);
      }
      
      SdkHttpRequest requestWithHeaders = requestBuilder.build();
      
      // Create canonical request (excluding x-amz-content-sha256 from headers)
      String canonicalRequest = createCanonicalRequest(requestWithHeaders);
      logger.debug("Canonical Request:\n{}", canonicalRequest);
      
      // Create string to sign
      String region = getAWSRegion().toString();
      String service = "sts";
      String credentialScope = dateStamp + "/" + region + "/" + service + "/aws4_request";
      String algorithm = "AWS4-HMAC-SHA256";
      
      String stringToSign = algorithm + "\n" +
          amzDate + "\n" +
          credentialScope + "\n" +
          sha256Hex(canonicalRequest);
      
      logger.debug("String to Sign:\n{}", stringToSign);
      
      // Calculate signature
      byte[] signingKey = getSignatureKey(secretKey, dateStamp, region, service);
      String signature = bytesToHex(hmacSha256(signingKey, stringToSign));
      
      // Create authorization header
      String signedHeaders = getSignedHeaders(requestWithHeaders);
      String authorizationHeader = algorithm + " " +
          "Credential=" + accessKey + "/" + credentialScope + ", " +
          "SignedHeaders=" + signedHeaders + ", " +
          "Signature=" + signature;
      
      logger.debug("Authorization Header: {}", authorizationHeader);
      
      // Return signed request
      return requestWithHeaders.toBuilder()
          .putHeader("Authorization", authorizationHeader)
          .build();
          
    } catch (Exception e) {
      logger.error("Failed to sign request", e);
      throw new RuntimeException("Failed to sign request", e);
    }
  }
  
  private String createCanonicalRequest(SdkHttpRequest request) {
    // HTTP method
    String method = request.method().toString();
    
    // Canonical URI
    String canonicalUri = request.getUri().getPath();
    if (canonicalUri.isEmpty()) {
      canonicalUri = "/";
    }
    
    // Canonical query string
    String canonicalQueryString = "";
    if (request.getUri().getQuery() != null) {
      canonicalQueryString = request.getUri().getQuery();
    }
    
    // Canonical headers (sorted, lowercase, exclude x-amz-content-sha256)
    Map<String, String> sortedHeaders = new TreeMap<>();
    for (Map.Entry<String, List<String>> entry : request.headers().entrySet()) {
      String headerName = entry.getKey().toLowerCase();
      // Exclude x-amz-content-sha256 from canonical headers
      if (!"x-amz-content-sha256".equals(headerName)) {
        String headerValue = String.join(",", entry.getValue()).trim();
        sortedHeaders.put(headerName, headerValue);
      }
    }
    
    StringBuilder canonicalHeaders = new StringBuilder();
    for (Map.Entry<String, String> entry : sortedHeaders.entrySet()) {
      canonicalHeaders.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
    }
    
    // Signed headers (exclude x-amz-content-sha256)
    String signedHeaders = String.join(";", sortedHeaders.keySet());
    
    // Payload hash - use empty string hash since we have no body
    String payloadHash = sha256Hex("");
    
    return method + "\n" +
        canonicalUri + "\n" +
        canonicalQueryString + "\n" +
        canonicalHeaders + "\n" +
        signedHeaders + "\n" +
        payloadHash;
  }
  
  private String getSignedHeaders(SdkHttpRequest request) {
    List<String> headerNames = new ArrayList<>();
    for (String headerName : request.headers().keySet()) {
      String lowerHeaderName = headerName.toLowerCase();
      // Exclude x-amz-content-sha256 from signed headers
      if (!"x-amz-content-sha256".equals(lowerHeaderName)) {
        headerNames.add(lowerHeaderName);
      }
    }
    Collections.sort(headerNames);
    return String.join(";", headerNames);
  }
  
  private byte[] getSignatureKey(String key, String dateStamp, String regionName, String serviceName) 
      throws Exception {
    byte[] kDate = hmacSha256(("AWS4" + key).getBytes(StandardCharsets.UTF_8), dateStamp);
    byte[] kRegion = hmacSha256(kDate, regionName);
    byte[] kService = hmacSha256(kRegion, serviceName);
    return hmacSha256(kService, "aws4_request");
  }
  
  private byte[] hmacSha256(byte[] key, String data) throws Exception {
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(key, "HmacSHA256"));
    return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
  }
  
  private String sha256Hex(String data) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(data.getBytes(StandardCharsets.UTF_8));
      return bytesToHex(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 not available", e);
    }
  }
  
  private String bytesToHex(byte[] bytes) {
    StringBuilder result = new StringBuilder();
    for (byte b : bytes) {
      result.append(String.format("%02x", b));
    }
    return result.toString();
  }

  AwsCredentials assumeRole(AwsCredentials currentCredentials, String roleArn) throws SFException {
    try (StsClient stsClient = createStsClient(currentCredentials, 10000)) {

      AssumeRoleRequest assumeRoleRequest =
          AssumeRoleRequest.builder()
              .roleArn(roleArn)
              .roleSessionName("identity-federation-session")
              .build();

      AssumeRoleResponse assumeRoleResponse = stsClient.assumeRole(assumeRoleRequest);
      Credentials credentials = assumeRoleResponse.credentials();

      logger.debug("Successfully assumed role: {}", roleArn);

      return AwsSessionCredentials.create(
          credentials.accessKeyId(), credentials.secretAccessKey(), credentials.sessionToken());

    } catch (Exception e) {
      logger.error("Failed to assume role: {} - {}", roleArn, e.getMessage());
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "Failed to assume AWS role " + roleArn + ": " + e.getMessage());
    }
  }

  AwsCredentials getCredentialsViaRoleChaining(SFLoginInput loginInput) throws SFException {
    AwsCredentials currentCredentials = getAWSCredentials();
    if (currentCredentials == null) {
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "No initial AWS credentials found for role chaining");
    }

    for (String roleArn : loginInput.getWorkloadIdentityImpersonationPath()) {
      logger.debug("Assuming role: {}", roleArn);
      currentCredentials = assumeRole(currentCredentials, roleArn);
      if (currentCredentials == null) {
        throw new SFException(
            ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR, "Failed to assume role: " + roleArn);
      }
    }

    return currentCredentials;
  }

  public String getCallerIdentityArn(AwsCredentials credentials, int timeoutMs) {
    if (credentials == null) {
      logger.debug("Cannot get caller identity with null credentials");
      return null;
    }

    Region region = getAWSRegion();
    if (region == null) {
      logger.debug("Cannot get caller identity without AWS region");
      return null;
    }

    try (StsClient stsClient = createStsClient(credentials, timeoutMs)) {
      GetCallerIdentityResponse callerIdentity = stsClient.getCallerIdentity();

      if (callerIdentity == null || callerIdentity.arn() == null) {
        logger.debug("GetCallerIdentity returned null or missing ARN");
        return null;
      }

      return callerIdentity.arn();

    } catch (Exception e) {
      logger.debug("Failed to get caller identity ARN: {}", e.getMessage());
      return null;
    }
  }

  private StsClient createStsClient(AwsCredentials credentials, int timeoutMs) {
    return StsClient.builder()
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .overrideConfiguration(config -> config.apiCallTimeout(Duration.ofMillis(timeoutMs)))
        .region(getAWSRegion())
        .build();
  }
}
