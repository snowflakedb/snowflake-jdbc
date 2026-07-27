package net.snowflake.client.internal.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import net.snowflake.client.api.exception.SnowflakeSQLException;
import org.junit.jupiter.api.Test;

public class ArrowResultChunkTest {
  @Test
  public void testEmptyChunkIterator() throws SnowflakeSQLException {
    ArrowResultChunk.ArrowChunkIterator iterator = ArrowResultChunk.getEmptyChunkIterator();

    assertThat(iterator.next(), is(false));
    assertThat(iterator.isAfterLast(), is(true));
    assertThat(iterator.isLast(), is(false));
  }
}
