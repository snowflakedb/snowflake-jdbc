package net.snowflake.client.internal.core.auth.wif;

import java.time.Duration;
import java.util.List;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SFLoginInput;
import net.snowflake.client.internal.jdbc.EnvironmentVariables;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;
import software.amazon.awssdk.services.sts.model.GetWebIdentityTokenRequest;
import software.amazon.awssdk.services.sts.model.GetWebIdentityTokenResponse;

public class AwsAttestationService {
  public static final SFLogger logger = SFLoggerFactory.getLogger(AwsAttestationService.class);
  public static final int TIMEOUT_MS = 10_000;
  private static final String SIGNING_ALGORITHM_ES384 = "ES384";
  private static Region region;

  void initializeSignerRegion() {
    logger.debug("Getting AWS region from environment variable");
    String envRegion = SnowflakeUtil.systemGetEnv(EnvironmentVariables.AWS_REGION.getName());
    if (envRegion != null) {
      region = Region.of(envRegion);
    } else {
      logger.debug("Getting AWS region from default region provider chain");
      region = DefaultAwsRegionProviderChain.builder().build().getRegion();
    }
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
    return region;
  }

  AwsCredentials assumeRole(AwsCredentials currentCredentials, String roleArn, String externalId)
      throws SFException {
    try (StsClient stsClient = createStsClient(currentCredentials, TIMEOUT_MS)) {

      AssumeRoleRequest.Builder assumeRoleRequestBuilder =
          AssumeRoleRequest.builder()
              .roleArn(roleArn)
              .roleSessionName("identity-federation-session");
      if (externalId != null && !externalId.isEmpty()) {
        assumeRoleRequestBuilder.externalId(externalId);
      }
      AssumeRoleRequest assumeRoleRequest = assumeRoleRequestBuilder.build();

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
    List<String> impersonationPath = loginInput.getWorkloadIdentityImpersonationPath();
    for (int i = 0; i < impersonationPath.size(); i++) {
      String roleArn = impersonationPath.get(i);
      logger.debug("Assuming role: {}", roleArn);
      String externalId =
          (i == impersonationPath.size() - 1)
              ? loginInput.getWorkloadIdentityAwsExternalId()
              : null;
      currentCredentials = assumeRole(currentCredentials, roleArn, externalId);
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

  String getWebIdentityToken(AwsCredentials credentials) throws SFException {
    try (StsClient stsClient = createStsClient(credentials, TIMEOUT_MS)) {
      GetWebIdentityTokenRequest request =
          GetWebIdentityTokenRequest.builder()
              .audience(WorkloadIdentityUtil.SNOWFLAKE_AUDIENCE)
              .signingAlgorithm(SIGNING_ALGORITHM_ES384)
              .build();
      GetWebIdentityTokenResponse response = stsClient.getWebIdentityToken(request);
      String jwt = response.webIdentityToken();
      if (jwt == null || jwt.isEmpty()) {
        throw new SFException(
            ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
            "AWS STS GetWebIdentityToken returned an empty token");
      }
      return jwt;
    } catch (Exception e) {
      logger.debug("AWS STS GetWebIdentityToken call failed", e);
      throw new SFException(
          e,
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "Failed to call AWS STS GetWebIdentityToken: " + e.getMessage());
    }
  }

  StsClient createStsClient(AwsCredentials credentials, int timeoutMs) {
    return StsClient.builder()
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .overrideConfiguration(config -> config.apiCallTimeout(Duration.ofMillis(timeoutMs)))
        .region(getAWSRegion())
        .build();
  }
}
