/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryOthers;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@Category(TestCategoryOthers.class)
public class RestRequestIT
{
  private CloseableHttpResponse retryResponse()
  {
    StatusLine retryStatusLine = mock(StatusLine.class);
    when(retryStatusLine.getStatusCode())
        .thenReturn(503);

    CloseableHttpResponse retryResponse = mock(CloseableHttpResponse.class);
    when(retryResponse.getStatusLine())
        .thenReturn(retryStatusLine);

    return retryResponse;
  }

  private CloseableHttpResponse successResponse()
  {
    StatusLine successStatusLine = mock(StatusLine.class);
    when(successStatusLine.getStatusCode())
        .thenReturn(200);

    CloseableHttpResponse successResponse = mock(CloseableHttpResponse.class);
    when(successResponse.getStatusLine())
        .thenReturn(successStatusLine);

    return successResponse;
  }

  private void execute(CloseableHttpClient client, String uri,
                       boolean includeRetryParameters)
  throws IOException, SnowflakeSQLException
  {
    RestRequest.execute(
        client,
        new HttpGet(uri),
        0, // retry timeout
        0, // inject socket timeout
        new AtomicBoolean(false), // canceling
        false, // without cookie
        includeRetryParameters,
        true,
        true);
  }

  @Test
  public void testRetryParamsInRequest() throws IOException, SnowflakeSQLException
  {
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(client.execute(any(HttpUriRequest.class)))
        .thenAnswer(new Answer<CloseableHttpResponse>()
        {
          int callCount = 0;

          @Override
          public CloseableHttpResponse answer(InvocationOnMock invocation)
          throws Throwable
          {
            HttpUriRequest arg = (HttpUriRequest) invocation.getArguments()[0];
            String params = arg.getURI().getQuery();

            if (callCount == 0)
            {
              assertFalse(params.contains("retryCount="));
              assertFalse(params.contains("clientStartTime="));
              assertTrue(params.contains("request_guid="));
            }
            else
            {
              assertTrue(params.contains("retryCount=" + callCount));
              assertTrue(params.contains("clientStartTime="));
              assertTrue(params.contains("request_guid="));
            }

            callCount += 1;
            if (callCount >= 3)
            {
              return successResponse();
            }
            else
            {
              return retryResponse();
            }
          }
        });

    execute(client, "fakeurl.com/?requestId=abcd-1234", true);
  }

  @Test
  public void testRetryNoParamsInRequest() throws IOException, SnowflakeSQLException
  {
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(client.execute(any(HttpUriRequest.class)))
        .thenAnswer(new Answer<CloseableHttpResponse>()
        {
          int callCount = 0;

          @Override
          public CloseableHttpResponse answer(InvocationOnMock invocation)
          throws Throwable
          {
            HttpUriRequest arg = (HttpUriRequest) invocation.getArguments()[0];
            String params = arg.getURI().getQuery();

            if (callCount > 0)
            {
              assertTrue(params.contains("retryCount=" + callCount));
            }
            assertFalse(params.contains("clientStartTime="));
            assertTrue(params.contains("request_guid="));

            callCount += 1;
            if (callCount >= 3)
            {
              return successResponse();
            }
            else
            {
              return retryResponse();
            }
          }
        });

    execute(client, "fakeurl.com/?requestId=abcd-1234", false);
  }
}
