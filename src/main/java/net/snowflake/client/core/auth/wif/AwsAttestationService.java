package net.snowflake.client.core.auth.wif;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Request;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.InstanceMetadataRegionProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.EnvironmentVariables;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class AwsAttestationService {

  public static final SFLogger logger = SFLoggerFactory.getLogger(AwsAttestationService.class);

  private static boolean regionInitialized = false;
  private static String region;

  private final AWS4Signer aws4Signer;

  public AwsAttestationService() {
    aws4Signer = new AWS4Signer();
  }

  void initializeSignerRegion() {
    aws4Signer.setRegionName(getAWSRegion());
  }

  public AWSCredentials getAWSCredentials() {
    try {
      return DefaultAWSCredentialsProviderChain.getInstance().getCredentials();
    } catch (Exception e) {
      logger.debug("Failed to retrieve AWS credentials: {}", e.getMessage());
      return null;
    }
  }

  String getAWSRegion() {
    if (!regionInitialized) {
      logger.debug("Getting AWS region from environment variable");
      String envRegion = SnowflakeUtil.systemGetEnv(EnvironmentVariables.AWS_REGION.getName());
      if (envRegion != null) {
        region = envRegion;
      } else {
        logger.debug("Getting AWS region from EC2 metadata service");
        region = new InstanceMetadataRegionProvider().getRegion();
      }
      regionInitialized = true;
    }
    return region;
  }

  public String getCallerIdentityArn(AWSCredentials credentials, int timeoutMs) {
    if (credentials == null) {
      logger.debug("Cannot get caller identity with null credentials");
      return null;
    }

    String region = getAWSRegion();
    if (region == null || region.trim().isEmpty()) {
      logger.debug("Cannot get caller identity without AWS region");
      return null;
    }

    AWSSecurityTokenService stsClient = null;
    try {
      stsClient = createStsClient(credentials, timeoutMs);
      GetCallerIdentityResult callerIdentity =
          stsClient.getCallerIdentity(new GetCallerIdentityRequest());

      if (callerIdentity == null || callerIdentity.getArn() == null) {
        logger.debug("GetCallerIdentity returned null or missing ARN");
        return null;
      }

      return callerIdentity.getArn();

    } catch (Exception e) {
      logger.debug("Failed to get caller identity ARN: {}", e.getMessage());
      return null;
    } finally {
      if (stsClient != null) {
        stsClient.shutdown();
      }
    }
  }

  void signRequestWithSigV4(Request<Void> signableRequest, AWSCredentials awsCredentials) {
    aws4Signer.setServiceName(signableRequest.getServiceName());
    aws4Signer.sign(signableRequest, awsCredentials);
  }

  AWSCredentials assumeRole(AWSCredentials currentCredentials, String roleArn) throws SFException {
    AWSSecurityTokenService stsClient = null;
    try {
      stsClient = createStsClient(currentCredentials, null);

      AssumeRoleRequest assumeRoleRequest =
          new AssumeRoleRequest()
              .withRoleArn(roleArn)
              .withRoleSessionName("identity-federation-session");

      AssumeRoleResult assumeRoleResult = stsClient.assumeRole(assumeRoleRequest);
      Credentials credentials = assumeRoleResult.getCredentials();

      logger.debug("Successfully assumed role: {}", roleArn);

      return new BasicSessionCredentials(
          credentials.getAccessKeyId(),
          credentials.getSecretAccessKey(),
          credentials.getSessionToken());

    } catch (Exception e) {
      logger.error("Failed to assume role: {} - {}", roleArn, e.getMessage());
      throw new SFException(
          ErrorCode.WORKLOAD_IDENTITY_FLOW_ERROR,
          "Failed to assume AWS role " + roleArn + ": " + e.getMessage());
    } finally {
      if (stsClient != null) {
        stsClient.shutdown();
      }
    }
  }

  AWSCredentials getCredentialsViaRoleChaining(SFLoginInput loginInput) throws SFException {
    AWSCredentials currentCredentials = getAWSCredentials();
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

  private AWSSecurityTokenService createStsClient(AWSCredentials credentials, Integer timeoutMs) {
    AWSSecurityTokenServiceClientBuilder builder =
        AWSSecurityTokenServiceClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(getAWSRegion());

    if (timeoutMs != null) {
      ClientConfiguration clientConfiguration = new ClientConfiguration();
      clientConfiguration.setConnectionTimeout(timeoutMs);
      clientConfiguration.setRequestTimeout(timeoutMs);
      clientConfiguration.setClientExecutionTimeout(timeoutMs);
      clientConfiguration.setMaxErrorRetry(0); // Single attempt, no retries
      builder.withClientConfiguration(clientConfiguration);
    }

    return builder.build();
  }
}
