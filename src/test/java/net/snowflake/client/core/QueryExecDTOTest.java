/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class QueryExecDTOTest {

  @Test
  public void testQueryExecSelect() {
    HashMap<String, Object> parameters = new HashMap<>();
    parameters.put("CLIENT_RESULT_CHUNK_SIZE", 48);

    QueryExecDTO body =
        new QueryExecDTO(
            "SELECT 1", false, 0, null, null, parameters, 1679518576917L, false, false);

    Map<String, ParameterBindingDTO> sampleBindings = new HashMap<>();
    ParameterBindingDTO param = new ParameterBindingDTO("FIXED", "1001");
    sampleBindings.put("1", param);
    parameters.put("QUERY_RESULT_FORMAT", "json");
    long currentTime = System.currentTimeMillis();

    body.setSqlText("");
    body.setDescribeOnly(true);
    body.setBindings(sampleBindings);
    body.setBindStage("/temp/stage");
    body.setParameters(parameters);
    body.setQuerySubmissionTime(currentTime);
    body.setIsInternal(true);
    body.setAsyncExec(true);
    body.setDescribedJobId("123456789");

    assertEquals("", body.getSqlText());
    assertTrue(body.isDescribeOnly());
    assertEquals(1, body.getBindings().size());
    assertEquals("/temp/stage", body.getBindStage());
    assertEquals(2, body.getParameters().size());
    assertEquals("json", body.getParameters().get("QUERY_RESULT_FORMAT"));
    assertEquals(currentTime, body.getQuerySubmissionTime());
    assertTrue(body.getIsInternal());
    assertTrue(body.getAsyncExec());
    assertEquals("123456789", body.getDescribedJobId());
  }
}
