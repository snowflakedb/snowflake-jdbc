package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.storage.StorageException;
import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.security.InvalidKeyException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletionException;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl;
import net.snowflake.client.internal.api.implementation.statement.SnowflakeStatementImpl;
import net.snowflake.client.internal.core.Constants;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.core.SFStatement;
import net.snowflake.client.internal.jdbc.cloud.storage.SnowflakeS3Client;
import net.snowflake.client.internal.jdbc.cloud.storage.StageInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Test for SnowflakeS3Client handle exception function */
@Tag(TestTags.OTHERS)
public class SnowflakeS3ClientHandleExceptionLatestIT extends AbstractDriverIT {
  @TempDir private File tmpFolder;
  private Connection connection;
  private SFStatement sfStatement;
  private SFSession sfSession;
  private String command;
  private SnowflakeS3Client spyingClient;
  private int overMaxRetry;
  private int maxRetry;
  private static final String EXPIRED_AWS_TOKEN_ERROR_CODE = "ExpiredToken";

  @BeforeEach
  public void setup() throws SQLException {
    connection = getConnection("s3testaccount");
    sfSession = connection.unwrap(SnowflakeConnectionImpl.class).getSfSession();
    Statement statement = connection.createStatement();
    sfStatement = statement.unwrap(SnowflakeStatementImpl.class).getSfStatement();
    statement.execute("CREATE OR REPLACE STAGE testPutGet_stage");
    command = "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @testPutGet_stage";
    SnowflakeFileTransferAgent agent =
        new SnowflakeFileTransferAgent(command, sfSession, sfStatement);
    StageInfo info = agent.getStageInfo();
    SnowflakeS3Client.ClientConfiguration clientConfig =
        new SnowflakeS3Client.ClientConfiguration(1, 1, 10_000, 10_000);
    SnowflakeS3Client client =
        new SnowflakeS3Client(
            info.getCredentials(),
            clientConfig,
            agent.getEncryptionMaterial().get(0),
            info.getProxyProperties(),
            info.getRegion(),
            info.getEndPoint(),
            info.getIsClientSideEncrypted(),
            sfSession,
            info.getUseS3RegionalUrl());
    maxRetry = client.getMaxRetries();
    overMaxRetry = maxRetry + 1;
    spyingClient = Mockito.spy(client);
  }

  @Test
  @DontRunOnGithubActions
  public void errorRenewExpired() throws SQLException, InterruptedException {
    CompletionException ex =
        new CompletionException(
            S3Exception.builder()
                .message("unauthenticated")
                .awsErrorDetails(
                    AwsErrorDetails.builder().errorCode(EXPIRED_AWS_TOKEN_ERROR_CODE).build())
                .build());
    spyingClient.handleStorageException(ex, 0, "upload", sfSession, command, null);
    Mockito.verify(spyingClient, Mockito.times(1)).renew(Mockito.anyMap());

    // Unauthenticated, backoff with interrupt, renew is called
    Exception[] exceptionContainer = new Exception[1];
    Thread thread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  spyingClient.handleStorageException(
                      ex, maxRetry, "upload", sfSession, command, null);
                } catch (SnowflakeSQLException e) {
                  exceptionContainer[0] = e;
                }
              }
            });
    thread.start();
    thread.interrupt();
    thread.join();
    assertNull(exceptionContainer[0], "Exception must not have been thrown in here");
    Mockito.verify(spyingClient, Mockito.times(2)).renew(Mockito.anyMap());
  }

  @Test
  @DontRunOnGithubActions
  public void errorNotFound() {
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                S3Exception.builder().message("Not found").build(),
                overMaxRetry,
                "upload",
                sfSession,
                command,
                null));
  }

  @Test
  @DontRunOnGithubActions
  public void errorBadRequestTokenExpired() throws SQLException {
    CompletionException ex =
        new CompletionException(
            SdkServiceException.builder().message("Bad Request").statusCode(400).build());
    spyingClient.handleStorageException(ex, 0, "download", sfSession, command, null);
    // renew token
    Mockito.verify(spyingClient, Mockito.times(1)).renew(Mockito.anyMap());
  }

  @Test
  @DontRunOnGithubActions
  public void errorClientUnknown() {
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                SdkClientException.create("Not found", new IOException()),
                overMaxRetry,
                "upload",
                sfSession,
                command,
                null));
  }

  @Test
  @DontRunOnGithubActions
  public void errorInvalidKey() {
    // Unauthenticated, renew is called.
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                new Exception(new InvalidKeyException()), 0, "upload", sfSession, command, null));
  }

  @Test
  @DontRunOnGithubActions
  public void errorInterruptedException() throws SnowflakeSQLException {
    // Can still retry, no error thrown
    spyingClient.handleStorageException(
        new InterruptedException(), 0, "upload", sfSession, command, null);

    Mockito.verify(spyingClient, Mockito.never()).renew(Mockito.anyMap());
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                new InterruptedException(), 26, "upload", sfSession, command, null));
  }

  @Test
  @DontRunOnGithubActions
  public void errorSocketTimeoutException() throws SnowflakeSQLException {
    // Can still retry, no error thrown
    spyingClient.handleStorageException(
        new SocketTimeoutException(), 0, "upload", sfSession, command, null);

    Mockito.verify(spyingClient, Mockito.never()).renew(Mockito.anyMap());
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                new SocketTimeoutException(), 26, "upload", sfSession, command, null));
  }

  @Test
  @DontRunOnGithubActions
  public void errorUnknownException() {
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                new Exception(), 0, "upload", sfSession, command, null));
  }

  @Test
  @DontRunOnGithubActions
  public void errorRenewExpiredNullSession() {
    // Unauthenticated, renew is called.
    CompletionException ex =
        new CompletionException(
            S3Exception.builder()
                .message("unauthenticated")
                .awsErrorDetails(
                    AwsErrorDetails.builder().errorCode(EXPIRED_AWS_TOKEN_ERROR_CODE).build())
                .build());
    assertThrows(
        SnowflakeSQLException.class,
        () -> spyingClient.handleStorageException(ex, 0, "upload", null, command, null));
  }

  @Test
  @DontRunOnGithubActions
  public void errorNoSpaceLeftOnDevice() throws IOException {
    File destFolder = new File(tmpFolder, "dest");
    destFolder.mkdirs();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    String getCommand =
        "get @testPutGet_stage/" + TEST_DATA_FILE + " 'file://" + destFolderCanonicalPath + "'";
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                new StorageException(
                    maxRetry,
                    Constants.NO_SPACE_LEFT_ON_DEVICE_ERR,
                    new IOException(Constants.NO_SPACE_LEFT_ON_DEVICE_ERR)),
                0,
                "download",
                null,
                getCommand,
                null));
  }

  @AfterEach
  public void cleanUp() throws SQLException {
    sfStatement.close();
    connection.close();
  }
}
