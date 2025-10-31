package net.snowflake.client.core.auth.wif;

import java.time.Duration;
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
    AwsCredentialsIdentity credentialsIdentity =
        AwsCredentialsIdentity.create(
            awsCredentials.accessKeyId(), awsCredentials.secretAccessKey());

    // Add session token header manually if credentials are session credentials
    if (awsCredentials instanceof AwsSessionCredentials) {
      AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) awsCredentials;
      signableRequest =
          signableRequest.toBuilder()
              .putHeader("X-Amz-Security-Token", sessionCredentials.sessionToken())
              .build();
    }

    SignRequest<AwsCredentialsIdentity> signRequest =
        SignRequest.builder(credentialsIdentity)
            .request(signableRequest)
            .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, "sts")
            .putProperty(AwsV4HttpSigner.REGION_NAME, getAWSRegion().toString())
            .build();

    return aws4Signer.sign(signRequest).request();
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
