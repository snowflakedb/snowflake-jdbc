/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

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
