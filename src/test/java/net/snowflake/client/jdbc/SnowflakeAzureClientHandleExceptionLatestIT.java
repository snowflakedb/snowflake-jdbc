/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobStorageException;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.client.core.Constants;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.cloud.storage.SnowflakeAzureClient;
import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Test for SnowflakeAzureClient handle exception function */
@Category(TestCategoryOthers.class)
public class SnowflakeAzureClientHandleExceptionLatestIT extends AbstractDriverIT {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  private Connection connection;
  private SFStatement sfStatement;
  private SFSession sfSession;
  private String command;
  private SnowflakeAzureClient spyingClient;
  private int overMaxRetry;
  private int maxRetry;

  @Before
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

  private HttpResponse constructHttpResponse(int httpStatus) {
    return new HttpResponse(null) {
      @Override
      public int getStatusCode() {
        return httpStatus;
      }

      @Override
      public String getHeaderValue(String s) {
        return "";
      }

      @Override
      public HttpHeaders getHeaders() {
        return null;
      }

      @Override
      public Flux<ByteBuffer> getBody() {
        return null;
      }

      @Override
      public Mono<byte[]> getBodyAsByteArray() {
        return null;
      }

      @Override
      public Mono<String> getBodyAsString() {
        return null;
      }

      @Override
      public Mono<String> getBodyAsString(Charset charset) {
        return null;
      }
    };
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void error403RenewExpired() throws SQLException, InterruptedException {
    // Unauthenticated, renew is called.
    spyingClient.handleStorageException(
        new BlobStorageException(
            "Unauthenticated", constructHttpResponse(HttpStatus.SC_FORBIDDEN), new Exception()),
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
                      new BlobStorageException(
                          "Unauthenticated",
                          constructHttpResponse(HttpStatus.SC_FORBIDDEN),
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
    Assert.assertNull("Exception must not have been thrown in here", exceptionContainer[0]);
    Mockito.verify(spyingClient, Mockito.times(4)).renew(Mockito.anyMap());
  }

  @Test(expected = SnowflakeSQLException.class)
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void error403OverMaxRetryThrow() throws SQLException {
    spyingClient.handleStorageException(
        new BlobStorageException(
            "Unauthenticated", constructHttpResponse(HttpStatus.SC_FORBIDDEN), new Exception()),
        overMaxRetry,
        "upload",
        sfSession,
        command,
        null);
  }

  @Test(expected = SnowflakeSQLException.class)
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void error403NullSession() throws SQLException {
    spyingClient.handleStorageException(
        new BlobStorageException(
            "Unauthenticated", constructHttpResponse(HttpStatus.SC_FORBIDDEN),  new Exception()),
        0,
        "upload",
        null,
        command,
        null);
  }

  @Test(expected = SnowflakeSQLException.class)
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void errorInvalidKey() throws SQLException {
    spyingClient.handleStorageException(
        new Exception(new InvalidKeyException()), 0, "upload", sfSession, command, null);
  }

  @Test(expected = SnowflakeSQLException.class)
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void errorInterruptedException() throws SQLException {
    // Can still retry, no error thrown
    try {
      spyingClient.handleStorageException(
          new InterruptedException(), 0, "upload", sfSession, command, null);
    } catch (Exception e) {
      Assert.fail("Should not have exception here");
    }
    Mockito.verify(spyingClient, Mockito.never()).renew(Mockito.anyMap());
    spyingClient.handleStorageException(
        new InterruptedException(), 26, "upload", sfSession, command, null);
  }

  @Test(expected = SnowflakeSQLException.class)
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void errorSocketTimeoutException() throws SQLException {
    // Can still retry, no error thrown
    try {
      spyingClient.handleStorageException(
          new SocketTimeoutException(), 0, "upload", sfSession, command, null);
    } catch (Exception e) {
      Assert.fail("Should not have exception here");
    }
    Mockito.verify(spyingClient, Mockito.never()).renew(Mockito.anyMap());
    spyingClient.handleStorageException(
        new SocketTimeoutException(), 26, "upload", sfSession, command, null);
  }

  @Test(expected = SnowflakeSQLException.class)
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void errorUnknownException() throws SQLException {
    spyingClient.handleStorageException(new Exception(), 0, "upload", sfSession, command, null);
  }

  @Test(expected = SnowflakeSQLException.class)
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void errorNoSpaceLeftOnDevice() throws SQLException, IOException {
    File destFolder = tmpFolder.newFolder();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    String getCommand =
        "get @testPutGet_stage/" + TEST_DATA_FILE + " 'file://" + destFolderCanonicalPath + "'";
    spyingClient.handleStorageException(
            new IOException(Constants.NO_SPACE_LEFT_ON_DEVICE_ERR),
        0,
        "download",
        null,
        getCommand,
        null);
  }

  @After
  public void cleanUp() throws SQLException {
    sfStatement.close();
    connection.close();
  }
}
