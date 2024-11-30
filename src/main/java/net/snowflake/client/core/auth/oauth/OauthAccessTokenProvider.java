package net.snowflake.client.core.auth.oauth;

import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;

public interface OauthAccessTokenProvider {

    String getAccessToken(SFLoginInput loginInput) throws SFException;
}
