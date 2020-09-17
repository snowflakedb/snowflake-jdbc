/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.util.Map;

/**
 * Body of a query request
 *
 * <p>Created by hyu on 6/30/17.
 */
public class QueryExecDTO {
  private String sqlText;

  @Deprecated private Integer sequenceId;

  private Map<String, ParameterBindingDTO> bindings;

  private String bindStage;

  private boolean describeOnly;

  private Map<String, Object> parameters;

  private String describedJobId;

  private long querySubmissionTime;

  private boolean isInternal;

  // Boolean value that, if true, indicates query should be asynchronous
  private boolean asyncExec;

  public QueryExecDTO(
      String sqlText,
      boolean describeOnly,
      Integer sequenceId,
      Map<String, ParameterBindingDTO> bindings,
      String bindStage,
      Map<String, Object> parameters,
      long querySubmissionTime,
      boolean internal,
      boolean asyncExec) {
    this.sqlText = sqlText;
    this.describeOnly = describeOnly;
    this.sequenceId = sequenceId;
    this.bindings = bindings;
    this.bindStage = bindStage;
    this.parameters = parameters;
    this.querySubmissionTime = querySubmissionTime;
    this.isInternal = internal;
    this.asyncExec = asyncExec; // indicates whether query should be asynchronous
  }

  @Deprecated
  public Integer getSequenceId() {
    return sequenceId;
  }

  @Deprecated
  public void setSequenceId(Integer sequenceId) {
    this.sequenceId = sequenceId;
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, Object> parameters) {
    this.parameters = parameters;
  }

  public void setDescribedJobId(String describedJobId) {
    this.describedJobId = describedJobId;
  }

}
