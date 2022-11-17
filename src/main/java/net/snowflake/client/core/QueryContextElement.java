/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.util.Arrays;

/** Query context information. */
class QueryContextElement implements Comparable<QueryContextElement> {
  long id; // database id as key. (bigint)
  long readTimestamp; // When the query context read (bigint). Compare for same id.
  long priority; // Priority of the query context (bigint). Compare for different ids.
  byte[] context; // Opaque information (varbinary).

  public QueryContextElement(long id, long readTimestamp, long priority, byte[] context) {
    this.id = id;
    this.readTimestamp = readTimestamp;
    this.priority = priority;
    this.context = context;
  }

  public boolean equals(Object obj) {
    if (obj == this) return true;

    if (!(obj instanceof QueryContextElement)) return super.equals(obj);

    QueryContextElement other = (QueryContextElement) obj;
    return (id == other.id
        && readTimestamp == other.readTimestamp
        && priority == other.priority
        && context.equals(other.context));
  }

  public int hashCode() {
    int hash = 31;

    hash = hash * 31 + (int) id;
    hash += (hash * 31) + (int) readTimestamp;
    hash += (hash * 31) + (int) priority;
    hash += (hash * 31) + Arrays.hashCode(context);

    return hash;
  }

  public int compareTo(QueryContextElement obj) {
    return (priority == obj.priority) ? 0 : (((priority - obj.priority) < 0) ? -1 : 1);
  }
}
