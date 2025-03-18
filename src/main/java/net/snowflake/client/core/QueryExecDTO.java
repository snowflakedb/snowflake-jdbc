package net.snowflake.client.core;

import java.util.Map;

/** Body of a query request */
public class QueryExecDTO {
  private String sqlText;

  @Deprecated private Integer sequenceId;

  private Map<String, ParameterBindingDTO> bindings;

  private String bindStage;

  private boolean describeOnly;

  private Map<String, Object> parameters;

  // Optional query context sent to the JDBC driver from the Cloud Service.
  private QueryContextDTO queryContextDTO;

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
      QueryContextDTO queryContext,
      long querySubmissionTime,
      boolean internal,
      boolean asyncExec) {
    this.sqlText = sqlText;
    this.describeOnly = describeOnly;
    this.sequenceId = sequenceId;
    this.bindings = bindings;
    this.bindStage = bindStage;
    this.parameters = parameters;
    this.queryContextDTO = queryContext;
    this.querySubmissionTime = querySubmissionTime;
    this.isInternal = internal;
    this.asyncExec = asyncExec; // indicates whether query should be asynchronous
  }

  public String getSqlText() {
    return sqlText;
  }

  public void setSqlText(String sqlText) {
    this.sqlText = sqlText;
  }

  @Deprecated
  public Integer getSequenceId() {
    return sequenceId;
  }

  @Deprecated
  public void setSequenceId(Integer sequenceId) {
    this.sequenceId = sequenceId;
  }

  public Map<String, ParameterBindingDTO> getBindings() {
    return bindings;
  }

  public void setBindings(Map<String, ParameterBindingDTO> bindings) {
    this.bindings = bindings;
  }

  public String getBindStage() {
    return bindStage;
  }

  public void setBindStage(String bindStage) {
    this.bindStage = bindStage;
  }

  public boolean isDescribeOnly() {
    return describeOnly;
  }

  public void setDescribeOnly(boolean describeOnly) {
    this.describeOnly = describeOnly;
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, Object> parameters) {
    this.parameters = parameters;
  }

  public QueryContextDTO getqueryContextDTO() {
    return queryContextDTO;
  }

  public void queryContextDTO(QueryContextDTO queryContext) {
    this.queryContextDTO = queryContext;
  }

  public String getDescribedJobId() {
    return describedJobId;
  }

  public void setDescribedJobId(String describedJobId) {
    this.describedJobId = describedJobId;
  }

  public long getQuerySubmissionTime() {
    return querySubmissionTime;
  }

  public void setQuerySubmissionTime(long querySubmissionTime) {
    this.querySubmissionTime = querySubmissionTime;
  }

  public void setIsInternal(boolean isInternal) {
    this.isInternal = isInternal;
  }

  public boolean getIsInternal() {
    return this.isInternal;
  }

  public void setAsyncExec(boolean asyncExec) {
    this.asyncExec = asyncExec;
  }

  public boolean getAsyncExec() {
    return this.asyncExec;
  }
}
