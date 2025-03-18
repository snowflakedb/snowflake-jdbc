package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageExtendedErrorInformation;
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
import net.snowflake.client.jdbc.cloud.storage.SnowflakeAzureClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/** Test for SnowflakeAzureClient handle exception function */
@Tag(TestTags.OTHERS)
public class SnowflakeAzureClientHandleExceptionLatestIT extends AbstractDriverIT {
  @TempDir private File tmpFolder;
  private Connection connection;
  private SFStatement sfStatement;
  private SFSession sfSession;
  private String command;
  private SnowflakeAzureClient spyingClient;
  private int overMaxRetry;
  private int maxRetry;

  @BeforeEach
  public void setup() throws SQLException {
    connection = getConnection("azureaccount");
    sfSession = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
    Statement statement = connection.createStatement();
    sfStatement = statement.unwrap(SnowflakeStatementV1.class).getSfStatement();
    statement.execute("CREATE OR REPLACE STAGE testPutGet_stage");
    command = "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @testPutGet_stage";
    SnowflakeFileTransferAgent agent =
        new SnowflakeFileTransferAgent(command, sfSession, sfStatement);
    SnowflakeAzureClient client =
        SnowflakeAzureClient.createSnowflakeAzureClient(
            agent.getStageInfo(), agent.getEncryptionMaterial().get(0), sfSession);
    maxRetry = client.getMaxRetries();
    overMaxRetry = maxRetry + 1;
    spyingClient = Mockito.spy(client);
  }

  @Test
  @DontRunOnGithubActions
  public void error403RenewExpired() throws SQLException, InterruptedException {
    // Unauthenticated, renew is called.
    spyingClient.handleStorageException(
        new StorageException(
            "403", "Unauthenticated", 403, new StorageExtendedErrorInformation(), new Exception()),
        0,
        "upload",
        sfSession,
        command,
        null);
    Mockito.verify(spyingClient, Mockito.times(2)).renew(Mockito.anyMap());

    // Unauthenticated, backoff with interrupt, renew is called
    Exception[] exceptionContainer = new Exception[1];
    Thread thread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  spyingClient.handleStorageException(
                      new StorageException(
                          "403",
                          "Unauthenticated",
                          403,
                          new StorageExtendedErrorInformation(),
                          new Exception()),
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
    Mockito.verify(spyingClient, Mockito.times(4)).renew(Mockito.anyMap());
  }

  @Test
  @DontRunOnGithubActions
  public void error403OverMaxRetryThrow() {
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                new StorageException(
                    "403",
                    "Unauthenticated",
                    403,
                    new StorageExtendedErrorInformation(),
                    new Exception()),
                overMaxRetry,
                "upload",
                sfSession,
                command,
                null));
  }

  @Test
  @DontRunOnGithubActions
  public void error403NullSession() {
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                new StorageException(
                    "403",
                    "Unauthenticated",
                    403,
                    new StorageExtendedErrorInformation(),
                    new Exception()),
                0,
                "upload",
                null,
                command,
                null));
  }

  @Test
  @DontRunOnGithubActions
  public void errorInvalidKey() {
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
    assertThrows(
        SnowflakeSQLException.class,
        () ->
            spyingClient.handleStorageException(
                new Exception(), 0, "upload", sfSession, command, null));
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
                    "",
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
