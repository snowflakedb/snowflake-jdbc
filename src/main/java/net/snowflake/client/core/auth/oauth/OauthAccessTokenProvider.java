package net.snowflake.client.core.auth.oauth;

import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public interface OauthAccessTokenProvider {

    String getAccessToken(SFLoginInput loginInput) throws SFException;
}
