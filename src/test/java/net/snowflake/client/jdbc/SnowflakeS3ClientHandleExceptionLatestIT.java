package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.cloud.storage.StorageException;
import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.security.InvalidKeyException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.Constants;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.cloud.storage.SnowflakeS3Client;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

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
    sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
    Statement statement = connection.createStatement();
    sfStatement = statement.unwrap(SnowflakeStatementV1.class).getSfStatement();
    statement.execute("CREATE OR REPLACE STAGE testPutGet_stage");
    command = "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @testPutGet_stage";
    SnowflakeFileTransferAgent agent =
        new SnowflakeFileTransferAgent(command, sfSession, sfStatement);
    StageInfo info = agent.getStageInfo();
    ClientConfiguration clientConfig = new ClientConfiguration();
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
    AmazonS3Exception ex = new AmazonS3Exception("unauthenticated");
    ex.setErrorCode(EXPIRED_AWS_TOKEN_ERROR_CODE);
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
                new AmazonS3Exception("Not found"),
                overMaxRetry,
                "upload",
                sfSession,
                command,
                null));
  }

  @Test
  @DontRunOnGithubActions
  public void errorBadRequestTokenExpired() throws SQLException {
    AmazonServiceException ex = new AmazonServiceException("Bad Request");
    ex.setServiceName("Amazon S3");
    ex.setStatusCode(400);
    ex.setErrorCode("400 Bad Request");
    ex.setErrorType(AmazonServiceException.ErrorType.Client);
    Mockito.doReturn(true).when(spyingClient).isClientException400Or404(ex);
    spyingClient.handleStorageException(ex, 0, "download", sfSession, command, null);
    // renew token
    Mockito.verify(spyingClient, Mockito.times(1)).isClientException400Or404(ex);
    Mockito.verify(spyingClient, Mockito.times(1)).renew(Mockito.anyMap());
  }

  @Test
  @DontRunOnGithubActions
  public void errorClientUnknown() {
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                new AmazonClientException("Not found", new IOException()),
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
    AmazonS3Exception ex = new AmazonS3Exception("unauthenticated");
    ex.setErrorCode(EXPIRED_AWS_TOKEN_ERROR_CODE);
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
