package net.snowflake.client.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

// The POJO object used by both JDBC and the Cloud service to exchange opaque informations.
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryContextDTO {

  // QueryContextDTO is a list of QueryContextEntryDTO. The first entry is the main entry with
  // priority 0.
  private List<QueryContextEntryDTO> entries;

  public QueryContextDTO() {
    entries = null;
  }

  public QueryContextDTO(List<QueryContextEntryDTO> entries) {
    this.entries = entries;
  }

  public List<QueryContextEntryDTO> getEntries() {
    return entries;
  }

  public void setEntries(List<QueryContextEntryDTO> entries) {
    this.entries = entries;
  }
}
