/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ArrowResultChunkTest
{
  @Test
  public void testEmptyChunkIterator() throws SnowflakeSQLException
  {
    ArrowResultChunk.ArrowChunkIterator iterator =
        ArrowResultChunk.getEmptyChunkIterator();

    assertThat(iterator.next(), is(false));
    assertThat(iterator.isAfterLast(), is(true));
    assertThat(iterator.isLast(), is(false));
  }
}
