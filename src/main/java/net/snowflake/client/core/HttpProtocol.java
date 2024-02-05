package net.snowflake.client.core;

public enum HttpProtocol {

    HTTP("http"),

    HTTPS("https");

    private final String protocol;

    HttpProtocol(String protocol) {
        this.protocol = protocol;
    }

    @Override
    public String toString() {
        return protocol;
    }
}
