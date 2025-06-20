package net.snowflake.client.core;

public class GrpcEndpointInfo {
    private final String host;
    private final int port;
    private final String executorId;
    // Add more fields as needed (e.g., channel, stub, etc.)

    public GrpcEndpointInfo(String host, int port, String executorId) {
        this.host = host;
        this.port = port;
        this.executorId = executorId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getExecutorId() {
        return executorId;
    }
} 