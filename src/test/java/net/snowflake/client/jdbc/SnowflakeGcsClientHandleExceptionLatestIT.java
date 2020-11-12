package net.snowflake.client.jdbc;

import com.google.cloud.storage.StorageException;
import java.net.SocketTimeoutException;
import java.security.InvalidKeyException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.cloud.storage.SnowflakeGCSClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/** Test for SnowflakeGcsClient handle exception function, only work with latest driver */
@Category(TestCategoryOthers.class)
public class SnowflakeGcsClientHandleExceptionLatestIT extends AbstractDriverIT {

  private Connection connection;
  private SFStatement sfStatement;
  private SFSession sfSession;
  private String command;
  private SnowflakeGCSClient spyingClient;
  private int overMaxRetry;
  private int maxRetry;

  @Before
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
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void error401RenewExpired() throws SQLException, InterruptedException {
    // Unauthenticated, renew is called.
    spyingClient.handleStorageException(
        new StorageException(401, "Unauthenticated"), 0, "upload", sfSession, command);
    Mockito.verify(spyingClient, Mockito.times(1)).renew(Mockito.anyMap());

    // Unauthenticated, session or command null, not renew, renew called remaining 1
    spyingClient.handleStorageException(
        new StorageException(401, "Unauthenticated"), 0, "upload", null, command);
    Mockito.verify(spyingClient, Mockito.times(1)).renew(Mockito.anyMap());
    spyingClient.handleStorageException(
        new StorageException(401, "Unauthenticated"), 0, "upload", sfSession, null);
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
                      command);
                } catch (SnowflakeSQLException e) {
                  exceptionContainer[0] = e;
                }
              }
            });
    thread.start();
    thread.interrupt();
    thread.join();
    Assert.assertNull("Exception must not have been thrown in here", exceptionContainer[0]);
    Mockito.verify(spyingClient, Mockito.times(2)).renew(Mockito.anyMap());
  }

  @Test(expected = SnowflakeSQLException.class)
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void error401OverMaxRetryThrow() throws SQLException {
    spyingClient.handleStorageException(
        new StorageException(401, "Unauthenticated"), overMaxRetry, "upload", sfSession, command);
  }

  @Test(expected = SnowflakeSQLException.class)
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void errorInvalidKey() throws SQLException {
    // Unauthenticated, renew is called.
    spyingClient.handleStorageException(
        new Exception(new InvalidKeyException()), 0, "upload", sfSession, command);
  }

  @Test(expected = SnowflakeSQLException.class)
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void errorInterruptedException() throws SQLException {
    // Can still retry, no error thrown
    try {
      spyingClient.handleStorageException(
          new InterruptedException(), 0, "upload", sfSession, command);
    } catch (Exception e) {
      Assert.fail("Should not have exception here");
    }
    Mockito.verify(spyingClient, Mockito.never()).renew(Mockito.anyMap());
    spyingClient.handleStorageException(
        new InterruptedException(), 26, "upload", sfSession, command);
  }

  @Test(expected = SnowflakeSQLException.class)
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void errorSocketTimeoutException() throws SQLException {
    // Can still retry, no error thrown
    try {
      spyingClient.handleStorageException(
          new SocketTimeoutException(), 0, "upload", sfSession, command);
    } catch (Exception e) {
      Assert.fail("Should not have exception here");
    }
    Mockito.verify(spyingClient, Mockito.never()).renew(Mockito.anyMap());
    spyingClient.handleStorageException(
        new SocketTimeoutException(), 26, "upload", sfSession, command);
  }

  @Test(expected = SnowflakeSQLException.class)
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void errorUnknownException() throws SQLException {
    // Unauthenticated, renew is called.
    spyingClient.handleStorageException(new Exception(), 0, "upload", sfSession, command);
  }

  @After
  public void cleanUp() throws SQLException {
    sfStatement.close();
    connection.close();
  }
}
