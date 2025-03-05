package net.snowflake.client.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

public class QueryContextCacheTest {
  private QueryContextCache qcc = null;
  private long BASE_READ_TIMESTAMP = 1668727958;
  private String CONTEXT = "Some query context";
  private long BASE_ID = 0;
  private long BASE_PRIORITY = 0;

  private int MAX_CAPACITY = 5;
  private long[] expectedIDs;
  private long[] expectedReadTimestamp;
  private long[] expectedPriority;

  private void initCache() {
    qcc = new QueryContextCache(MAX_CAPACITY);
  }

  private void initCacheWithData() {
    initCacheWithDataWithContext(CONTEXT);
  }

  private void initCacheWithDataWithContext(String context) {
    qcc = new QueryContextCache(MAX_CAPACITY);
    expectedIDs = new long[MAX_CAPACITY];
    expectedReadTimestamp = new long[MAX_CAPACITY];
    expectedPriority = new long[MAX_CAPACITY];
    for (int i = 0; i < MAX_CAPACITY; i++) {
      expectedIDs[i] = BASE_ID + i;
      expectedReadTimestamp[i] = BASE_READ_TIMESTAMP + i;
      expectedPriority[i] = BASE_PRIORITY + i;
      qcc.merge(expectedIDs[i], expectedReadTimestamp[i], expectedPriority[i], context);
    }
    qcc.syncPriorityMap();
  }

  private void initCacheWithDataInRandomOrder() {
    qcc = new QueryContextCache(MAX_CAPACITY);
    expectedIDs = new long[MAX_CAPACITY];
    expectedReadTimestamp = new long[MAX_CAPACITY];
    expectedPriority = new long[MAX_CAPACITY];
    for (int i = 0; i < MAX_CAPACITY; i++) {
      expectedIDs[i] = BASE_ID + i;
      expectedReadTimestamp[i] = BASE_READ_TIMESTAMP + i;
      expectedPriority[i] = BASE_PRIORITY + i;
    }

    qcc.merge(expectedIDs[3], expectedReadTimestamp[3], expectedPriority[3], CONTEXT);
    qcc.merge(expectedIDs[2], expectedReadTimestamp[2], expectedPriority[2], CONTEXT);
    qcc.merge(expectedIDs[4], expectedReadTimestamp[4], expectedPriority[4], CONTEXT);
    qcc.merge(expectedIDs[0], expectedReadTimestamp[0], expectedPriority[0], CONTEXT);
    qcc.merge(expectedIDs[1], expectedReadTimestamp[1], expectedPriority[1], CONTEXT);
    qcc.syncPriorityMap();
  }

  /** Test for empty cache */
  @Test
  public void testIsEmpty() throws Exception {
    initCache();
    assertThat("Empty cache", qcc.getSize() == 0);
  }

  @Test
  public void testWithSomeData() throws Exception {
    initCacheWithData();
    // Compare elements
    assertCacheData();
  }

  @Test
  public void testWithSomeDataInRandomOrder() throws Exception {
    initCacheWithDataInRandomOrder();
    // Compare elements
    assertCacheData();
  }

  @Test
  public void testMoreThanCapacity() throws Exception {
    initCacheWithData();

    // Add one more element at the end
    int i = MAX_CAPACITY;
    qcc.merge(BASE_ID + i, BASE_READ_TIMESTAMP + i, BASE_PRIORITY + i, CONTEXT);
    qcc.syncPriorityMap();
    qcc.checkCacheCapacity();

    // Compare elements
    assertCacheData();
  }

  @Test
  public void testUpdateTimestamp() throws Exception {
    initCacheWithData();

    // Add one more element with new TS with existing id
    int updatedID = 1;
    expectedReadTimestamp[updatedID] = BASE_READ_TIMESTAMP + updatedID + 10;
    qcc.merge(
        BASE_ID + updatedID, expectedReadTimestamp[updatedID], BASE_PRIORITY + updatedID, CONTEXT);
    qcc.syncPriorityMap();
    qcc.checkCacheCapacity();

    // Compare elements
    assertCacheData();
  }

  @Test
  public void testUpdatePriority() throws Exception {
    initCacheWithData();

    // Add one more element with new priority with existing id
    int updatedID = 3;
    long updatedPriority = BASE_PRIORITY + updatedID + 7;

    expectedPriority[updatedID] = updatedPriority;
    qcc.merge(
        BASE_ID + updatedID, BASE_READ_TIMESTAMP + updatedID, expectedPriority[updatedID], CONTEXT);
    qcc.syncPriorityMap();
    qcc.checkCacheCapacity();

    for (int i = updatedID; i < MAX_CAPACITY - 1; i++) {
      expectedIDs[i] = expectedIDs[i + 1];
      expectedReadTimestamp[i] = expectedReadTimestamp[i + 1];
      expectedPriority[i] = expectedPriority[i + 1];
    }

    expectedIDs[MAX_CAPACITY - 1] = BASE_ID + updatedID;
    expectedReadTimestamp[MAX_CAPACITY - 1] = BASE_READ_TIMESTAMP + updatedID;
    expectedPriority[MAX_CAPACITY - 1] = updatedPriority;

    assertCacheData();
  }

  @Test
  public void testAddSamePriority() throws Exception {
    initCacheWithData();

    // Add one more element with same priority
    int i = MAX_CAPACITY;
    long UpdatedPriority = BASE_PRIORITY + 1;
    qcc.merge(BASE_ID + i, BASE_READ_TIMESTAMP + i, UpdatedPriority, CONTEXT);
    qcc.syncPriorityMap();
    qcc.checkCacheCapacity();
    expectedIDs[1] = BASE_ID + i;
    expectedReadTimestamp[1] = BASE_READ_TIMESTAMP + i;

    // Compare elements
    assertCacheData();
  }

  @Test
  public void testAddSameIDButStaleTimestamp() throws Exception {
    initCacheWithData();

    // Add one more element with same priority
    int i = 2;
    qcc.merge(BASE_ID + i, BASE_READ_TIMESTAMP + i - 10, BASE_PRIORITY + i, CONTEXT);
    qcc.syncPriorityMap();
    qcc.checkCacheCapacity();

    // Compare elements
    assertCacheData();
  }

  @Test
  public void testEmptyCacheWithNullData() throws Exception {
    initCacheWithData();

    qcc.deserializeQueryContextJson(null);
    assertThat("Empty cache", qcc.getSize() == 0);
  }

  @Test
  public void testEmptyCacheWithEmptyResponseData() throws Exception {
    initCacheWithData();

    qcc.deserializeQueryContextJson("");
    assertThat("Empty cache", qcc.getSize() == 0);
  }

  @Test
  public void testSerializeRequestAndDeserializeResponseData() throws Exception {
    // Init qcc
    initCacheWithData();
    assertCacheData();

    QueryContextDTO requestData = qcc.serializeQueryContextDTO();

    // Clear qcc
    qcc.clearCache();
    assertThat("Empty cache", qcc.getSize() == 0);

    qcc.deserializeQueryContextDTO(requestData);
    assertCacheData();
  }

  @Test
  public void testSerializeRequestAndDeserializeResponseDataWithNullContext() throws Exception {
    // Init qcc
    initCacheWithDataWithContext(null);
    assertCacheDataWithContext(null);

    QueryContextDTO requestData = qcc.serializeQueryContextDTO();

    // Clear qcc
    qcc.clearCache();
    assertThat("Empty cache", qcc.getSize() == 0);

    qcc.deserializeQueryContextDTO(requestData);
    assertCacheDataWithContext(null);

    QueryContextCache mockQcc = spy(qcc);
    mockQcc.deserializeQueryContextDTO(null);
    verify(mockQcc).clearCache();
    verify(mockQcc, times(2)).logCacheEntries();
  }

  private void assertCacheData() {
    assertCacheDataWithContext(CONTEXT);
  }

  private void assertCacheDataWithContext(String context) {
    int size = qcc.getSize();
    assertThat("Non empty cache", size == MAX_CAPACITY);

    long[] ids = new long[size];
    long[] readTimestamps = new long[size];
    long[] priorities = new long[size];
    String[] contexts = new String[size];

    // Compare elements
    qcc.getElements(ids, readTimestamps, priorities, contexts);
    for (int i = 0; i < size; i++) {
      assertEquals(expectedIDs[i], ids[i]);
      assertEquals(expectedReadTimestamp[i], readTimestamps[i]);
      assertEquals(expectedPriority[i], priorities[i]);
      assertEquals(context, contexts[i]);
    }
  }
}
