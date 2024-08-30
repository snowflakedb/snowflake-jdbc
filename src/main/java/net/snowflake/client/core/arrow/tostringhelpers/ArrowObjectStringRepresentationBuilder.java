package net.snowflake.client.core.arrow.tostringhelpers;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeType;

@SnowflakeJdbcInternalApi
public class ArrowObjectStringRepresentationBuilder extends ArrowStringRepresentationBuilderBase {

    public ArrowObjectStringRepresentationBuilder() {
        super();
        this.append('{');
    }

    public ArrowStringRepresentationBuilderBase appendKeyValue(String key, String value, SnowflakeType valueType) {
        addCommaIfNeeded();
        this.appendQuoted(key).append(": ");
        if (shouldQuoteValue(valueType)) {
            return this.appendQuoted(value);
        }

        return this.append(value);
    }

    @Override
    public String toString() {
        this.append('}');
        return super.toString();
    }
}
