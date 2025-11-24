package net.snowflake.client.internal.jdbc.cloud.storage;

import static net.snowflake.client.internal.core.Constants.CLOUD_STORAGE_CREDENTIALS_EXPIRED;

import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.exception.SnowflakeSQLLoggedException;
import net.snowflake.client.internal.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;
import org.apache.http.HttpStatus;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3ErrorHandler {
  private static final SFLogger logger = SFLoggerFactory.getLogger(S3ErrorHandler.class);

  static void retryRequestWithExponentialBackoff(
      Exception ex,
      int retryCount,
      String operation,
      SFSession session,
      String command,
      SnowflakeStorageClient s3Client,
      String queryId,
      Throwable cause)
      throws SnowflakeSQLException {
    logger.debug(
        "Encountered exception ({}) during {}, retry count: {}",
        ex.getMessage(),
        operation,
        retryCount);
    logger.debug("Stack trace: ", ex);

    // exponential backoff up to a limit
    int backoffInMillis = s3Client.getRetryBackoffMin();

    if (retryCount > 1) {
      backoffInMillis <<= (Math.min(retryCount - 1, s3Client.getRetryBackoffMaxExponent()));
    }

    try {
      logger.debug("Sleep for {} milliseconds before retry", backoffInMillis);

      Thread.sleep(backoffInMillis);
    } catch (InterruptedException ex1) {
      // ignore
    }

    // If the exception indicates that the AWS token has expired,
    // we need to refresh our S3 client with the new token
    if (cause instanceof S3Exception) {
      S3Exception e = (S3Exception) cause;
      if (e.awsErrorDetails()
          .errorCode()
          .equalsIgnoreCase(SnowflakeS3Client.EXPIRED_AWS_TOKEN_ERROR_CODE)) {
        // If session is null we cannot renew the token so throw the ExpiredToken exception
        if (session != null) {
          SnowflakeFileTransferAgent.renewExpiredToken(session, command, s3Client);
        } else {
          throw new SnowflakeSQLException(
              queryId,
              e.awsErrorDetails().errorCode(),
              CLOUD_STORAGE_CREDENTIALS_EXPIRED,
              "S3 credentials have expired");
        }
      }
    }
  }

  static void throwIfClientExceptionOrMaxRetryReached(
      String operation,
      SFSession session,
      String command,
      String queryId,
      SnowflakeStorageClient s3Client,
      Throwable cause)
      throws SnowflakeSQLException {
    String extendedRequestId = "none";

    if (cause instanceof S3Exception) {
      S3Exception e = (S3Exception) cause;
      extendedRequestId = e.extendedRequestId();
    }

    if (cause instanceof SdkServiceException) {
      SdkServiceException ex1 = (SdkServiceException) cause;

      // The AWS credentials might have expired when server returns error 400 and
      // does not return the ExpiredToken error code.
      // If session is null we cannot renew the token so throw the exception
      if (ex1.statusCode() == HttpStatus.SC_BAD_REQUEST && session != null) {
        SnowflakeFileTransferAgent.renewExpiredToken(session, command, s3Client);
      } else {
        throw new SnowflakeSQLLoggedException(
            queryId,
            session,
            SqlState.SYSTEM_ERROR,
            StorageHelper.getOperationException(operation).getMessageCode(),
            ex1,
            operation,
            ex1.getMessage(),
            ex1.requestId(),
            extendedRequestId);
      }
    } else {
      throw new SnowflakeSQLLoggedException(
          queryId,
          session,
          SqlState.SYSTEM_ERROR,
          StorageHelper.getOperationException(operation).getMessageCode(),
          cause,
          operation,
          cause.getMessage());
    }
  }
}
