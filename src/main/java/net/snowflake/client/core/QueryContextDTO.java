package net.snowflake.client.core;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryContextDTO {

    private QueryContextEntryDTO mainEntry;
    private List<QueryContextEntryDTO> entries;

    public QueryContextDTO() {
        mainEntry = null;
        entries = null;
    }

    public QueryContextDTO(QueryContextEntryDTO mainEntry, List<QueryContextEntryDTO> entries) {
        this.mainEntry = mainEntry;
        this.entries = entries;
    }

    public QueryContextEntryDTO getMainEntry() {
        return mainEntry;
    }

    public void setMainEntry(QueryContextEntryDTO mainEntry) {
        this.mainEntry = mainEntry;
    }

    public List<QueryContextEntryDTO> getEntries() {
        return entries;
    }

    public void setEntries(List<QueryContextEntryDTO> entries) {
        this.entries = entries;
    }
}
