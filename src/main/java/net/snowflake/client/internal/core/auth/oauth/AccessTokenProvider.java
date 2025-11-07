package net.snowflake.client.internal.core.auth.oauth;

import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SFLoginInput;
import net.snowflake.client.internal.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public interface AccessTokenProvider {

  TokenResponseDTO getAccessToken(SFLoginInput loginInput) throws SFException;

  String getDPoPPublicKey();
}
