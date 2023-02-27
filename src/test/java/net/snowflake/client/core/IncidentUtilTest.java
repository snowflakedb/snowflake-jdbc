/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.junit.Test;

public class IncidentUtilTest {

  @Test
  public void testOneLinerDescription() {
    IOException ex = new IOException("File not found");
    String desc = IncidentUtil.oneLiner("unexpected exception", ex);
    assertEquals("unexpected exception java.io.IOException: File not found", desc.substring(0, 56));
  }
}
