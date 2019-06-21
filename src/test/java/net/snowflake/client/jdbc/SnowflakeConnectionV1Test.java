package net.snowflake.client.jdbc;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;
import java.util.Properties;

/**
 * Created by hyu on 2/2/18.
 */
public class SnowflakeConnectionV1Test
{
  @Test
  public void testMergeProperties()
  {
    // testcase 1
    String url = "jdbc:snowflake://testaccount.localhost:8080";
    Properties prop = new Properties();
    prop.put("account", "s3testaccount");
    prop.put("user", "snowman");

    Map<String, Object> result = SnowflakeConnectionV1.mergeProperties(url, prop);

    assertThat(result.size(), is(3));
    assertThat((String) result.get("ACCOUNT"), is("s3testaccount"));
    assertThat((String) result.get("USER"), is("snowman"));
    assertThat((String) result.get("SERVERURL"), is("https://testaccount.localhost:8080"));

    // testcase 2
    url = "jdbc:snowflake://testaccount.localhost:8080/?";
    prop = new Properties();
    prop.put("account", "s3testaccount");
    prop.put("user", "snowman");

    result = SnowflakeConnectionV1.mergeProperties(url, prop);

    assertThat(result.size(), is(3));
    assertThat((String) result.get("ACCOUNT"), is("s3testaccount"));
    assertThat((String) result.get("USER"), is("snowman"));
    assertThat((String) result.get("SERVERURL"), is("https://testaccount.localhost:8080/"));

    // testcase 3
    url = "jdbc:snowflake://testaccount.localhost:8080/?aaaa";
    prop = new Properties();
    prop.put("account", "s3testaccount");
    prop.put("user", "snowman");

    result = SnowflakeConnectionV1.mergeProperties(url, prop);

    assertThat(result.size(), is(3));
    assertThat((String) result.get("ACCOUNT"), is("s3testaccount"));
    assertThat((String) result.get("USER"), is("snowman"));
    assertThat((String) result.get("SERVERURL"), is("https://testaccount.localhost:8080/"));

    // testcase 4
    url = "jdbc:snowflake://testaccount.localhost:8080/?prop1=value1";
    prop = new Properties();
    prop.put("account", "s3testaccount");
    prop.put("user", "snowman");

    result = SnowflakeConnectionV1.mergeProperties(url, prop);

    assertThat(result.size(), is(4));
    assertThat((String) result.get("ACCOUNT"), is("s3testaccount"));
    assertThat((String) result.get("USER"), is("snowman"));
    assertThat((String) result.get("SERVERURL"), is("https://testaccount.localhost:8080/"));
    assertThat((String) result.get("PROP1"), is("value1"));

    // testcase 5
    url = "jdbc:snowflake://testaccount.localhost:8080/?prop1=value1&ssl=off";
    prop = new Properties();
    prop.put("account", "s3testaccount");
    prop.put("user", "snowman");

    result = SnowflakeConnectionV1.mergeProperties(url, prop);

    assertThat(result.size(), is(4));
    assertThat((String) result.get("ACCOUNT"), is("s3testaccount"));
    assertThat((String) result.get("USER"), is("snowman"));
    assertThat((String) result.get("SERVERURL"), is("http://testaccount.localhost:8080/"));
    assertThat((String) result.get("PROP1"), is("value1"));

    // testcase 6
    url = "jdbc:snowflake://testaccount.localhost:8080/?prop1=value1";
    prop = new Properties();
    prop.put("account", "s3testaccount");
    prop.put("user", "snowman");
    prop.put("ssl", "false");

    result = SnowflakeConnectionV1.mergeProperties(url, prop);

    assertThat(result.size(), is(4));
    assertThat((String) result.get("ACCOUNT"), is("s3testaccount"));
    assertThat((String) result.get("USER"), is("snowman"));
    assertThat((String) result.get("SERVERURL"), is("http://testaccount.localhost:8080/"));
    assertThat((String) result.get("PROP1"), is("value1"));

    // testcase 7
    url = "jdbc:snowflake://testaccount.localhost:8080/?prop1=value1";
    prop = new Properties();
    prop.put("user", "snowman");
    prop.put("ssl", "false");
    prop.put("prop1", "value2");

    result = SnowflakeConnectionV1.mergeProperties(url, prop);

    assertThat(result.size(), is(4));
    assertThat((String) result.get("ACCOUNT"), is("testaccount"));
    assertThat((String) result.get("USER"), is("snowman"));
    assertThat((String) result.get("SERVERURL"), is("http://testaccount.localhost:8080/"));
    assertThat((String) result.get("PROP1"), is("value2"));

    // testcase 8 (Global URL with no account specified)
    url = "jdbc:snowflake://testaccount-1234567890qwertyupalsjhfg" +
          ".global.snowflakecomputing.com:8080/?prop1=value";
    prop = new Properties();
    prop.put("user", "snowman");

    result = SnowflakeConnectionV1.mergeProperties(url, prop);

    assertThat(result.size(), is(4));
    assertThat((String) result.get("ACCOUNT"), is("testaccount"));
    assertThat((String) result.get("USER"), is("snowman"));
    assertThat((String) result.get("SERVERURL"), is("https://testaccount-1234567890qwertyupalsjhfg.global.snowflakecomputing.com:8080/"));
    assertThat((String) result.get("PROP1"), is("value"));

    // testcase 9 (Global URL with account specified)
    url = "jdbc:snowflake://testaccount-1234567890qwertyupalsjhfg" +
          ".global.snowflakecomputing.com:8080/?prop1=value";
    prop = new Properties();
    prop.put("user", "snowman");
    prop.put("account", "s3testaccount");

    result = SnowflakeConnectionV1.mergeProperties(url, prop);

    assertThat(result.size(), is(4));
    assertThat((String) result.get("ACCOUNT"), is("s3testaccount"));
    assertThat((String) result.get("USER"), is("snowman"));
    assertThat((String) result.get("SERVERURL"), is("https://testaccount-1234567890qwertyupalsjhfg.global.snowflakecomputing.com:8080/"));
    assertThat((String) result.get("PROP1"), is("value"));

    // test case when http is already embedded in URL
    url = "jdbc:snowflake://http://testaccount.localhost:8080/?prop1=value1";
    prop = new Properties();

    result = SnowflakeConnectionV1.mergeProperties(url, prop);
    assertThat((String) result.get("SERVERURL"), is("https://testaccount.localhost:8080/"));

    url = "jdbc:snowflake://https://testaccount.localhost:8080/?prop1=value1";
    prop = new Properties();

    result = SnowflakeConnectionV1.mergeProperties(url, prop);
    assertThat((String) result.get("SERVERURL"), is("https://testaccount.localhost:8080/"));
  }
}
