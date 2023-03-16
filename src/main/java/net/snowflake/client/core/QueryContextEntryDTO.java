package net.snowflake.client.core;



public class QueryContextEntryDTO {
    private Long id;
    private Long timestamp;
    private Long priority;
    private OpaqueContextDTO context;

    public QueryContextEntryDTO() {
        // empty constructor
    }

    public QueryContextEntryDTO(Long id, Long timestamp, Long priority, OpaqueContextDTO context) {
        this.id = id;
        this.timestamp = timestamp;
        this.priority = priority;
        this.context = context;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getPriority() {
        return priority;
    }

    public void setPriority(Long priority) {
        this.priority = priority;
    }

    public OpaqueContextDTO getContext() {
        return context;
    }

    public void setContext(OpaqueContextDTO context) {
        this.context = context;
    }
}
