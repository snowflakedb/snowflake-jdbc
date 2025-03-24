package net.snowflake.client.jdbc;

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
import java.util.Properties;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.Constants;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.cloud.storage.SnowflakeGCSClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/** Test for SnowflakeGcsClient handle exception function, only work with latest driver */
@Tag(TestTags.OTHERS)
public class SnowflakeGcsClientHandleExceptionLatestIT extends AbstractDriverIT {
  @TempDir private File tmpFolder;
  private Connection connection;
  private SFStatement sfStatement;
  private SFSession sfSession;
  private String command;
  private SnowflakeGCSClient spyingClient;
  private int overMaxRetry;
  private int maxRetry;

  @BeforeEach
  public void setup() throws SQLException {
    Properties paramProperties = new Properties();
    paramProperties.put("GCS_USE_DOWNSCOPED_CREDENTIAL", true);
    connection = getConnection("gcpaccount", paramProperties);
    sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
    Statement statement = connection.createStatement();
    sfStatement = statement.unwrap(SnowflakeStatementV1.class).getSfStatement();
    statement.execute("CREATE OR REPLACE STAGE testPutGet_stage");
    command = "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @testPutGet_stage";
    SnowflakeFileTransferAgent agent =
        new SnowflakeFileTransferAgent(command, sfSession, sfStatement);
    SnowflakeGCSClient client =
        SnowflakeGCSClient.createSnowflakeGCSClient(
            agent.getStageInfo(), agent.getEncryptionMaterial().get(0), sfSession);
    maxRetry = client.getMaxRetries();
    overMaxRetry = maxRetry + 1;
    spyingClient = Mockito.spy(client);
  }

  @Test
  @DontRunOnGithubActions
  public void error401RenewExpired() throws SQLException, InterruptedException {
    // Unauthenticated, renew is called.
    spyingClient.handleStorageException(
        new StorageException(401, "Unauthenticated"), 0, "upload", sfSession, command, null);
    Mockito.verify(spyingClient, Mockito.times(1)).renew(Mockito.anyMap());

    // Unauthenticated, command null, not renew, renew called remaining 1
    spyingClient.handleStorageException(
        new StorageException(401, "Unauthenticated"), 0, "upload", sfSession, null, null);
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
                      new StorageException(401, "Unauthenticated"),
                      maxRetry,
                      "upload",
                      sfSession,
                      command,
                      null);
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
  public void error401OverMaxRetryThrow() {
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                new StorageException(401, "Unauthenticated"),
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
  public void errorInterruptedException() throws SQLException {
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
    // Unauthenticated, renew is called.
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                new Exception(), 0, "upload", sfSession, command, null));
  }

  @Test
  @DontRunOnGithubActions
  public void errorWithNullSession() {
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                new StorageException(401, "Unauthenticated"), 0, "upload", null, command, null));
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
        SQLException.class,
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
