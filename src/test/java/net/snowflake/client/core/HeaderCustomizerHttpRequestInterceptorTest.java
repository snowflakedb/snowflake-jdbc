package net.snowflake.client.core;

import static net.snowflake.client.core.AttributeEnhancingHttpRequestRetryHandler.EXECUTION_COUNT_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.Request;
import com.amazonaws.http.HttpMethodName;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.jdbc.HttpHeadersCustomizer;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.RequestLine;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HeaderCustomizerHttpRequestInterceptorTest {
  private HttpHeadersCustomizer mockCustomizer1;
  private HttpHeadersCustomizer mockCustomizer2;
  private HttpRequest mockHttpRequest;
  private RequestLine mockRequestLine;
  private Request<?> mockAwsRequest;

  private HttpContext httpContext;
  private HeaderCustomizerHttpRequestInterceptor interceptor;
  private List<HttpHeadersCustomizer> customizersList;

  private final String TEST_URI_STRING = "https://test.snowflakecomputing.com/api/v1";
  private final URI TEST_URI = URI.create(TEST_URI_STRING);
  private final String TEST_METHOD_GET = "GET";

  @BeforeEach
  void setUp() {
    httpContext = new BasicHttpContext();
    customizersList = new ArrayList<>();
    // Default interceptor has empty list, tests will add customizers as needed
    interceptor = new HeaderCustomizerHttpRequestInterceptor(customizersList);

    // Setup common mocks
    mockCustomizer1 = mock();
    mockCustomizer2 = mock();
    mockHttpRequest = mock();
    mockRequestLine = mock();
    mockAwsRequest = mock();

    lenient().when(mockHttpRequest.getRequestLine()).thenReturn(mockRequestLine);
    lenient().when(mockHttpRequest.getAllHeaders()).thenReturn(new Header[0]);
    lenient().when(mockRequestLine.getMethod()).thenReturn(TEST_METHOD_GET);
    lenient().when(mockRequestLine.getUri()).thenReturn(TEST_URI_STRING);

    lenient().when(mockAwsRequest.getHttpMethod()).thenReturn(HttpMethodName.GET);
    lenient().when(mockAwsRequest.getEndpoint()).thenReturn(TEST_URI);
    lenient()
        .when(mockAwsRequest.getHeaders())
        .thenReturn(new HashMap<>()); // Mutable map for testing adds
  }

  @Test
  void testApacheInterceptorWithoutCustomizersDoesNothing() throws Exception {
    interceptor = new HeaderCustomizerHttpRequestInterceptor(Collections.emptyList());
    interceptor.process(mockHttpRequest, httpContext);
    verify(mockHttpRequest, never()).addHeader(anyString(), anyString());
  }

  @Test
  void testApacheInterceptorWithCustomizerWhichDoesNotApplyDoesNothing() throws Exception {
    when(mockCustomizer1.applies(anyString(), anyString(), anyMap())).thenReturn(false);
    customizersList.add(mockCustomizer1);
    interceptor = new HeaderCustomizerHttpRequestInterceptor(customizersList);

    interceptor.process(mockHttpRequest, httpContext);

    verify(mockCustomizer1).applies(eq(TEST_METHOD_GET), eq(TEST_URI_STRING), anyMap());
    verify(mockCustomizer1, never()).newHeaders();
    verify(mockHttpRequest, never()).addHeader(anyString(), anyString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testApacheInterceptorWithCustomizerAddsHeaders(boolean invokeOnce) throws Exception {
    Map<String, List<String>> newHeaders = new HashMap<>();
    newHeaders.put("X-Static", Collections.singletonList("StaticVal"));
    when(mockCustomizer1.applies(anyString(), anyString(), anyMap())).thenReturn(true);
    when(mockCustomizer1.newHeaders()).thenReturn(newHeaders);
    when(mockCustomizer1.invokeOnce()).thenReturn(invokeOnce);
    customizersList.add(mockCustomizer1);
    interceptor = new HeaderCustomizerHttpRequestInterceptor(customizersList);

    httpContext.setAttribute(EXECUTION_COUNT_ATTRIBUTE, 0);

    interceptor.process(mockHttpRequest, httpContext);

    verify(mockCustomizer1).newHeaders();
    verify(mockHttpRequest).addHeader("X-Static", "StaticVal");
  }

  @Test
  void testApacheInterceptorWithInvokeOnceTrueSkipsOnRetry() throws Exception {
    when(mockCustomizer1.applies(anyString(), anyString(), anyMap())).thenReturn(true);
    when(mockCustomizer1.invokeOnce()).thenReturn(true);
    customizersList.add(mockCustomizer1);
    interceptor = new HeaderCustomizerHttpRequestInterceptor(customizersList);

    httpContext.setAttribute(EXECUTION_COUNT_ATTRIBUTE, 1); // Simulate retry

    interceptor.process(mockHttpRequest, httpContext);

    verify(mockCustomizer1, never()).newHeaders();
    verify(mockHttpRequest, never()).addHeader(anyString(), anyString());
  }

  @Test
  void testApacheInterceptorWithInvokeOnceFalseAddsHeaderOnRetry() throws Exception {
    Map<String, List<String>> newHeaders = new HashMap<>();
    newHeaders.put("X-Dynamic", Collections.singletonList("RetryValue"));
    when(mockCustomizer1.applies(anyString(), anyString(), anyMap())).thenReturn(true);
    when(mockCustomizer1.newHeaders()).thenReturn(newHeaders);
    when(mockCustomizer1.invokeOnce()).thenReturn(false);
    customizersList.add(mockCustomizer1);
    interceptor = new HeaderCustomizerHttpRequestInterceptor(customizersList);

    httpContext.setAttribute(EXECUTION_COUNT_ATTRIBUTE, 1); // Simulate retry

    interceptor.process(mockHttpRequest, httpContext);

    verify(mockCustomizer1).newHeaders();
    verify(mockHttpRequest).addHeader("X-Dynamic", "RetryValue");
  }

  @Test
  void testApacheInterceptorDoesNotAllowCustomizerToOverrideHeader()
      throws HttpException, IOException {
    // Simulate driver adding User-Agent initially
    Header[] initialHeaders = {new BasicHeader("User-Agent", "SnowflakeDriver/1.0")};
    when(mockHttpRequest.getAllHeaders()).thenReturn(initialHeaders);

    Map<String, List<String>> newHeaders = new HashMap<>();
    newHeaders.put("User-Agent", Collections.singletonList("MaliciousAgent/2.0"));

    when(mockCustomizer1.applies(anyString(), anyString(), anyMap())).thenReturn(true);
    when(mockCustomizer1.newHeaders()).thenReturn(newHeaders); // Attempting override
    customizersList.add(mockCustomizer1);
    interceptor = new HeaderCustomizerHttpRequestInterceptor(customizersList);

    httpContext.setAttribute(EXECUTION_COUNT_ATTRIBUTE, 0);

    interceptor.process(mockHttpRequest, httpContext);

    // Verify the original map wasn't modified with the bad header
    assertEquals(initialHeaders, mockHttpRequest.getAllHeaders());
    verify(mockHttpRequest, never()).addHeader(eq("User-Agent"), anyString());
  }

  @Test
  void testMultipleCustomizersAddingHeaders() throws Exception {
    Map<String, List<String>> headers1 = new HashMap<>();
    headers1.put("X-Custom1", Collections.singletonList("Val1"));
    Map<String, List<String>> headers2 = new HashMap<>();
    headers2.put("X-Custom2", Arrays.asList("Val2a", "Val2b"));

    when(mockCustomizer1.applies(anyString(), anyString(), anyMap())).thenReturn(true);
    when(mockCustomizer1.newHeaders()).thenReturn(headers1);
    when(mockCustomizer1.invokeOnce()).thenReturn(false);

    when(mockCustomizer2.applies(anyString(), anyString(), anyMap())).thenReturn(true);
    when(mockCustomizer2.newHeaders()).thenReturn(headers2);
    when(mockCustomizer2.invokeOnce()).thenReturn(false);

    customizersList.add(mockCustomizer1);
    customizersList.add(mockCustomizer2);
    interceptor = new HeaderCustomizerHttpRequestInterceptor(customizersList);

    httpContext.setAttribute(EXECUTION_COUNT_ATTRIBUTE, 0);

    interceptor.process(mockHttpRequest, httpContext);

    verify(mockCustomizer1).newHeaders();
    verify(mockCustomizer2).newHeaders();
    verify(mockHttpRequest).addHeader("X-Custom1", "Val1");
    verify(mockHttpRequest).addHeader("X-Custom2", "Val2a");
    verify(mockHttpRequest).addHeader("X-Custom2", "Val2b");
  }

  @Test
  void testAWSInterceptorWithoutCustomizersDoesNothing() {
    interceptor = new HeaderCustomizerHttpRequestInterceptor(Collections.emptyList());
    interceptor.beforeRequest(mockAwsRequest);
    verify(mockAwsRequest, never()).addHeader(anyString(), anyString());
  }

  @Test
  void testAWSInterceptorWithCustomizerWhichDoesNotApplyDoesNothing() {
    when(mockCustomizer1.applies(anyString(), anyString(), anyMap())).thenReturn(false);
    customizersList.add(mockCustomizer1);
    interceptor = new HeaderCustomizerHttpRequestInterceptor(customizersList);

    interceptor.beforeRequest(mockAwsRequest);

    verify(mockCustomizer1).applies(eq(TEST_METHOD_GET), eq(TEST_URI_STRING), anyMap());
    verify(mockCustomizer1, never()).newHeaders();
    verify(mockAwsRequest, never()).addHeader(anyString(), anyString());
  }

  @Test
  void testAWSInterceptorWithCustomizerAddsHeaders() {
    Map<String, List<String>> newHeaders = new HashMap<>();
    newHeaders.put("X-AWS-Custom", Collections.singletonList("AwsValue1"));
    when(mockCustomizer1.applies(anyString(), anyString(), anyMap())).thenReturn(true);
    when(mockCustomizer1.newHeaders()).thenReturn(newHeaders);
    customizersList.add(mockCustomizer1);
    interceptor = new HeaderCustomizerHttpRequestInterceptor(customizersList);

    interceptor.beforeRequest(mockAwsRequest);

    verify(mockCustomizer1).newHeaders();
    verify(mockAwsRequest).addHeader("X-AWS-Custom", "AwsValue1");
  }

  @Test
  void testAWSInterceptorDoesNotAllowCustomizerToOverrideHeader() {
    // Simulate driver adding User-Agent initially
    Map<String, String> initialAwsHeaders = new HashMap<>();
    initialAwsHeaders.put("User-Agent", "SnowflakeAWSClient/1.0");
    when(mockAwsRequest.getHeaders()).thenReturn(initialAwsHeaders); // Return mutable map for test

    Map<String, List<String>> newHeaders = new HashMap<>();
    newHeaders.put("User-Agent", Collections.singletonList("MaliciousAgent/3.0"));
    when(mockCustomizer1.applies(anyString(), anyString(), anyMap())).thenReturn(true);
    when(mockCustomizer1.newHeaders()).thenReturn(newHeaders);
    customizersList.add(mockCustomizer1);
    interceptor = new HeaderCustomizerHttpRequestInterceptor(customizersList);

    interceptor.beforeRequest(mockAwsRequest);

    // Verify the original map wasn't modified with the bad header
    assertEquals("SnowflakeAWSClient/1.0", mockAwsRequest.getHeaders().get("User-Agent"));
    verify(mockHttpRequest, never()).addHeader(eq("User-Agent"), anyString());
  }

  @Test
  void testAWSInterceptorAddsMultiValueHeader() {
    Map<String, List<String>> newHeaders = new HashMap<>();
    newHeaders.put("X-Multi", Arrays.asList("ValA", "ValB"));
    when(mockCustomizer1.applies(anyString(), anyString(), anyMap())).thenReturn(true);
    when(mockCustomizer1.newHeaders()).thenReturn(newHeaders);
    customizersList.add(mockCustomizer1);
    interceptor = new HeaderCustomizerHttpRequestInterceptor(customizersList);

    interceptor.beforeRequest(mockAwsRequest);

    verify(mockCustomizer1).newHeaders();
    verify(mockAwsRequest).addHeader("X-Multi", "ValA");
    verify(mockAwsRequest).addHeader("X-Multi", "ValB");
  }
}
