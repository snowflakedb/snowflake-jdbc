package net.snowflake.client.internal.jdbc;

import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link RestRequest#executeWithRetries} releases the underlying HTTP connection
 * back to the pool on the failure-side exhaustion break, while NOT releasing it on a successful
 * 200 response (where the caller still owns the response stream).
 *
 * <p>See PR https://github.com/snowflakedb/snowflake-jdbc/pull/2643.
 */
@Tag(TestTags.OTHERS)
public class RestRequestConnectionReleaseWiremockLatestIT extends BaseWiremockTest {

  @Test
  public void releasesConnectionWhenExhaustionEndsInNon200Response() throws Exception {
    // implemented in Task 2
  }

  @Test
  public void releasesConnectionWhenExhaustionEndsInNullResponse() throws Exception {
    // implemented in Task 3
  }

  @Test
  public void doesNotReleaseConnectionImmediatelyOnSuccessfulResponse() throws Exception {
    // implemented in Task 4
  }
}
