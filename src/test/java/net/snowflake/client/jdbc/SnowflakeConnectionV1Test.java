package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.DefaultSFConnectionHandler.mergeProperties;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;
import java.util.Properties;
import org.junit.Test;

/** Created by hyu on 2/2/18. */
public class SnowflakeConnectionV1Test {

  @Test
  public void testMergeProperties() {
    SnowflakeConnectString conStr;
    Map<String, Object> result;

    // testcase 1

    Properties prop = new Properties();
    prop.put("account", "s3testaccount");
    prop.put("user", "snowman");
    conStr = SnowflakeConnectString.parse("jdbc:snowflake://testaccount.localhost:8080", prop);
    result = mergeProperties(conStr);

    assertThat(result.size(), is(3));
    assertThat(result.get("ACCOUNT"), is("s3testaccount"));
    assertThat(result.get("USER"), is("snowman"));
    assertThat(result.get("SERVERURL"), is("https://testaccount.localhost:8080/"));

    // testcase 2
    prop = new Properties();
    prop.put("account", "s3testaccount");
    prop.put("user", "snowman");
    conStr = SnowflakeConnectString.parse("jdbc:snowflake://testaccount.localhost:8080/?", prop);

    result = mergeProperties(conStr);

    assertThat(result.size(), is(3));
    assertThat(result.get("ACCOUNT"), is("s3testaccount"));
    assertThat(result.get("USER"), is("snowman"));
    assertThat(result.get("SERVERURL"), is("https://testaccount.localhost:8080/"));

    // testcase 3
    prop = new Properties();
    prop.put("account", "s3testaccount");
    prop.put("user", "snowman");
    conStr =
        SnowflakeConnectString.parse("jdbc:snowflake://testaccount.localhost:8080/?aaaa", prop);
    result = mergeProperties(conStr);

    assertThat(result.size(), is(3));
    assertThat(result.get("ACCOUNT"), is("s3testaccount"));
    assertThat(result.get("USER"), is("snowman"));
    assertThat(result.get("SERVERURL"), is("https://testaccount.localhost:8080/"));

    // testcase 4
    prop = new Properties();
    prop.put("account", "s3testaccount");
    prop.put("user", "snowman");
    conStr =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://testaccount.localhost:8080/?prop1=value1", prop);
    result = mergeProperties(conStr);

    assertThat(result.size(), is(4));
    assertThat(result.get("ACCOUNT"), is("s3testaccount"));
    assertThat(result.get("USER"), is("snowman"));
    assertThat(result.get("SERVERURL"), is("https://testaccount.localhost:8080/"));
    assertThat(result.get("PROP1"), is("value1"));

    // testcase 5
    prop = new Properties();
    prop.put("account", "s3testaccount");
    prop.put("user", "snowman");
    conStr =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://testaccount.localhost:8080/?prop1=value1&ssl=off", prop);
    result = mergeProperties(conStr);

    assertThat(result.size(), is(4));
    assertThat(result.get("ACCOUNT"), is("s3testaccount"));
    assertThat(result.get("USER"), is("snowman"));
    assertThat(result.get("SERVERURL"), is("http://testaccount.localhost:8080/"));
    assertThat(result.get("PROP1"), is("value1"));

    // testcase 6
    prop = new Properties();
    prop.put("account", "s3testaccount");
    prop.put("user", "snowman");
    prop.put("ssl", Boolean.FALSE.toString());
    conStr =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://testaccount.localhost:8080/?prop1=value1", prop);
    result = mergeProperties(conStr);

    assertThat(result.size(), is(4));
    assertThat(result.get("ACCOUNT"), is("s3testaccount"));
    assertThat(result.get("USER"), is("snowman"));
    assertThat(result.get("SERVERURL"), is("http://testaccount.localhost:8080/"));
    assertThat(result.get("PROP1"), is("value1"));

    // testcase 7
    prop = new Properties();
    prop.put("user", "snowman");
    prop.put("ssl", Boolean.FALSE.toString());
    prop.put("prop1", "value2");
    conStr =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://testaccount.localhost:8080/?prop1=value1", prop);
    result = mergeProperties(conStr);

    assertThat(result.size(), is(4));
    assertThat(result.get("ACCOUNT"), is("testaccount"));
    assertThat(result.get("USER"), is("snowman"));
    assertThat(result.get("SERVERURL"), is("http://testaccount.localhost:8080/"));
    assertThat(result.get("PROP1"), is("value2"));

    // testcase 8 (Global URL with no account specified)
    prop = new Properties();
    prop.put("user", "snowman");
    conStr =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://testaccount-1234567890qwertyupalsjhfg"
                + ".global.snowflakecomputing.com:8080/?prop1=value",
            prop);
    result = mergeProperties(conStr);

    assertThat(result.size(), is(4));
    assertThat(result.get("ACCOUNT"), is("testaccount"));
    assertThat(result.get("USER"), is("snowman"));
    assertThat(
        result.get("SERVERURL"),
        is("https://testaccount-1234567890qwertyupalsjhfg.global.snowflakecomputing.com:8080/"));
    assertThat(result.get("PROP1"), is("value"));

    // testcase 9 (Global URL with account specified)
    prop = new Properties();
    prop.put("user", "snowman");
    prop.put("account", "s3testaccount");
    conStr =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://testaccount-1234567890qwertyupalsjhfg"
                + ".global.snowflakecomputing.com:8080/?prop1=value",
            prop);
    result = mergeProperties(conStr);

    assertThat(result.size(), is(4));
    assertThat(result.get("ACCOUNT"), is("s3testaccount"));
    assertThat(result.get("USER"), is("snowman"));
    assertThat(
        result.get("SERVERURL"),
        is("https://testaccount-1234567890qwertyupalsjhfg.global.snowflakecomputing.com:8080/"));
    assertThat(result.get("PROP1"), is("value"));

    // testcase 10 (Global URL with dashed account in URL)
    prop = new Properties();
    prop.put("user", "snowman");
    conStr =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://test-account-1234567890qwertyupalsjhfg"
                + ".global.snowflakecomputing.com:8080/?prop1=value",
            prop);
    result = mergeProperties(conStr);

    assertThat(result.size(), is(4));
    assertThat(result.get("ACCOUNT"), is("test-account"));
    assertThat(result.get("USER"), is("snowman"));
    assertThat(
        result.get("SERVERURL"),
        is("https://test-account-1234567890qwertyupalsjhfg.global.snowflakecomputing.com:8080/"));
    assertThat(result.get("PROP1"), is("value"));

    // test case when http is already embedded in URL
    prop = new Properties();
    conStr =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://http://testaccount.localhost:8080/?prop1=value1", prop);
    result = mergeProperties(conStr);
    assertThat(result.get("SERVERURL"), is("http://testaccount.localhost:8080/"));

    prop = new Properties();
    conStr =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://https://testaccount.localhost:8080/?prop1=value1", prop);
    result = mergeProperties(conStr);
    assertThat(result.get("SERVERURL"), is("https://testaccount.localhost:8080/"));

    // test case for escaped characters
    prop = new Properties();
    conStr =
        SnowflakeConnectString.parse(
            "jdbc:snowflake://http://testaccount"
                + ".localhost:8080/?prop1=value1%7Cvalue2&prop2=carrot%5E",
            prop);
    result = mergeProperties(conStr);
    assertThat(result.get("PROP1"), is("value1|value2"));
    assertThat(result.get("PROP2"), is("carrot^"));
  }
}
