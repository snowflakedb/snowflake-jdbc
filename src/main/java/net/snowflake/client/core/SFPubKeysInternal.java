package net.snowflake.client.core;

class SSDPubKey
{
	private static final String pem_key_dep1 = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAy9QkFIGxs8oXnuUKeIzT\nNJ3l1aFIfoUuIiRtLJ1XwmyPYHnLjC0yye3smMmctx6BcXTV9E0ebf8a0sENhSDm\nThjFM62baNka23Pzo6cSSSGbT2m1NQbARKa4dNP7zkWIPHa2tuK1/jRCy6Z/ARTd\nkPgYa4Xr0br/vL3QoZ/sy2ieeT2U4Xa03jAghU9VgFYkIp3hpI6aTaDmKG8Z5mVj\novBpW8Rg0vkkwZ3GhjhAJhr6qwMoTSgkQU/Xst0X8duO/HD7bH9NYpsySMiU4+lR\nsrCC0rhiCToT36kidynajEJI6uQoTQzsPtFM+Nz0Vd1+dZfJ1H+ZyIROyVXlCKhR\nCQIDAQAB\n-----END PUBLIC KEY-----\n";

	private static final String pem_key_dep2 = null;

	static String getPublicKeyInternal(String dep)
	{
		if (dep.equals("dep1"))
		{
			return pem_key_dep1;
		}
		else
		{
			return pem_key_dep2;
		}
	}
}
