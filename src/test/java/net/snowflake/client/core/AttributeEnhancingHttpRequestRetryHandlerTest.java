package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AttributeEnhancingHttpRequestRetryHandlerTest {
  private final IOException dummyException = new IOException("Test exception");

  @ParameterizedTest
  @ValueSource(ints = {0, 3, 5, 10})
  void testAttributeSet(int executionCount) {
    HttpContext context = new BasicHttpContext();
    AttributeEnhancingHttpRequestRetryHandler handler =
        new AttributeEnhancingHttpRequestRetryHandler();

    handler.retryRequest(dummyException, executionCount, context);

    assertEquals(
        executionCount,
        context.getAttribute(AttributeEnhancingHttpRequestRetryHandler.EXECUTION_COUNT_ATTRIBUTE),
        "Attribute should be set to the provided executionCount: " + executionCount);
  }
}
