/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.util.Map;

/**
 * Body of a query request
 *
 * Created by hyu on 6/30/17.
 */
class QueryExecDTO
{
  private String sqlText;

  @Deprecated
  private Integer sequenceId;

  private Map<String, ParameterBindingDTO> bindings;

  private boolean describeOnly;

  private Map<String, Object> parameters;

  private String describedJobId;

  QueryExecDTO(String sqlText,
               boolean describeOnly,
               Integer sequenceId,
               Map<String, ParameterBindingDTO> bindings,
               Map<String, Object> parameters)
  {
    this.sqlText = sqlText;
    this.describeOnly = describeOnly;
    this.sequenceId = sequenceId;
    this.bindings = bindings;
    this.parameters = parameters;
  }

  public String getSqlText()
  {
    return sqlText;
  }

  public void setSqlText(String sqlText)
  {
    this.sqlText = sqlText;
  }

  @Deprecated
  public Integer getSequenceId()
  {
    return sequenceId;
  }

  @Deprecated
  public void setSequenceId(Integer sequenceId)
  {
    this.sequenceId = sequenceId;
  }

  public Map<String, ParameterBindingDTO> getBindings()
  {
    return bindings;
  }

  public void setBindings(Map<String, ParameterBindingDTO> bindings)
  {
    this.bindings = bindings;
  }

  public boolean isDescribeOnly()
  {
    return describeOnly;
  }

  public void setDescribeOnly(boolean describeOnly)
  {
    this.describeOnly = describeOnly;
  }

  public Map<String, Object> getParameters()
  {
    return parameters;
  }

  public void setParameters(Map<String, Object> parameters)
  {
    this.parameters = parameters;
  }

  public String getDescribedJobId()
  {
    return describedJobId;
  }

  public void setDescribedJobId(String describedJobId)
  {
    this.describedJobId = describedJobId;
  }
}
