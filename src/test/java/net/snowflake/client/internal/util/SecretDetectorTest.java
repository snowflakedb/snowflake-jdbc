package net.snowflake.client.internal.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.snowflake.client.internal.core.ObjectMapperFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

public class SecretDetectorTest {
  @Test
  public void testMaskAWSSecret() {
    String sql =
        "copy into 's3://xxxx/test' from \n"
            + "(select seq1(), random()\n"
            + ", random(), random(), random(), random()\n"
            + ", random(), random(), random(), random()\n"
            + ", random() , random(), random(), random()\n"
            + "\tfrom table(generator(rowcount => 10000)))\n"
            + "credentials=(\n"
            + "  aws_key_id='xxdsdfsafds'\n"
            + "  aws_secret_key='safas+asfsad+safasf'\n"
            + "  )\n"
            + "OVERWRITE = TRUE \n"
            + "MAX_FILE_SIZE = 500000000 \n"
            + "HEADER = TRUE \n"
            + "FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE )\n"
            + ";";
    String correct =
        "copy into 's3://xxxx/test' from \n"
            + "(select seq1(), random()\n"
            + ", random(), random(), random(), random()\n"
            + ", random(), random(), random(), random()\n"
            + ", random() , random(), random(), random()\n"
            + "\tfrom table(generator(rowcount => 10000)))\n"
            + "credentials=(\n"
            + "  aws_key_id='****'\n"
            + "  aws_secret_key='****'\n"
            + "  )\n"
            + "OVERWRITE = TRUE \n"
            + "MAX_FILE_SIZE = 500000000 \n"
            + "HEADER = TRUE \n"
            + "FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE )\n"
            + ";";
    String masked = SecretDetector.maskSecrets(sql);
    assertThat("secret masked", correct.compareTo(masked) == 0);
  }

  @Test
  public void testMaskSASToken() {
    // Initializing constants
    final String azureSasToken =
        "https://someaccounts.blob.core.windows.net/results/018b90ab-0033-"
            + "5f8e-0000-14f1000bd376_0/main/data_0_0_1?sv=2015-07-08&amp;"
            + "sig=iCvQmdZngZNW%2F4vw43j6%2BVz6fndHF5LI639QJba4r8o%3D&amp;"
            + "spr=https&amp;st=2016-04-12T03%3A24%3A31Z&amp;"
            + "se=2016-04-13T03%3A29%3A31Z&amp;srt=s&amp;ss=bf&amp;sp=rwl";

    final String maskedAzureSasToken =
        "https://someaccounts.blob.core.windows.net/results/018b90ab-0033-"
            + "5f8e-0000-14f1000bd376_0/main/data_0_0_1?sv=2015-07-08&amp;"
            + "sig=****&amp;"
            + "spr=https&amp;st=2016-04-12T03%3A24%3A31Z&amp;"
            + "se=2016-04-13T03%3A29%3A31Z&amp;srt=s&amp;ss=bf&amp;sp=rwl";

    final String s3SasToken =
        "https://somebucket.s3.amazonaws.com/vzy1-s-va_demo0/results/018b92f3"
            + "-01c2-02dd-0000-03d5000c8066_0/main/data_0_0_1?"
            + "x-amz-server-side-encryption-customer-algorithm=AES256&"
            + "response-content-encoding=gzip&AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE"
            + "&Expires=1555481960&Signature=zFiRkdB9RtRRYomppVes4fQ%2ByWw%3D";

    final String maskedS3SasToken =
        "https://somebucket.s3.amazonaws.com/vzy1-s-va_demo0/results/018b92f3"
            + "-01c2-02dd-0000-03d5000c8066_0/main/data_0_0_1?"
            + "x-amz-server-side-encryption-customer-algorithm=AES256&"
            + "response-content-encoding=gzip&AWSAccessKeyId=****"
            + "&Expires=1555481960&Signature=****";

    assertThat(
        "Azure SAS token is not masked",
        maskedAzureSasToken.equals(SecretDetector.maskSecrets(azureSasToken)));

    assertThat(
        "S3 SAS token is not masked",
        maskedS3SasToken.equals(SecretDetector.maskSecrets(s3SasToken)));

    String randomString = RandomStringUtils.random(200);
    assertThat(
        "Text without secrets is not unmodified",
        randomString.equals(SecretDetector.maskSecrets(randomString)));

    assertThat(
        "Text with 2 Azure SAS tokens is not masked",
        (maskedAzureSasToken + maskedAzureSasToken)
            .equals(SecretDetector.maskSecrets(azureSasToken + azureSasToken)));

    assertThat(
        "Text with 2 S3 SAS tokens is not masked",
        (maskedAzureSasToken + maskedAzureSasToken)
            .equals(SecretDetector.maskSecrets(azureSasToken + azureSasToken)));

    assertThat(
        "Text with Azure and S3 SAS tokens is not masked",
        (maskedAzureSasToken + maskedS3SasToken)
            .equals(SecretDetector.maskSecrets(azureSasToken + s3SasToken)));
  }

  @Test
  public void testMaskSecrets() {
    // Text containing AWS secret and Azure SAS token
    final String sqlText =
        "create stage mystage "
            + "URL = 's3://mybucket/mypath/' "
            + "credentials = (aws_key_id = 'AKIAIOSFODNN7EXAMPLE' "
            + "aws_secret_key = 'frJIUN8DYpKDtOLCwo//yllqDzg='); "
            + "create stage mystage2 "
            + "URL = 'azure//mystorage.blob.core.windows.net/cont' "
            + "credentials = (azure_sas_token = "
            + "'?sv=2016-05-31&ss=b&srt=sco&sp=rwdl&se=2018-06-27T10:05:50Z&"
            + "st=2017-06-27T02:05:50Z&spr=https,http&"
            + "sig=bgqQwoXwxzuD2GJfagRg7VOS8hzNr3QLT7rhS8OFRLQ%3D')";

    final String maskedSqlText =
        "create stage mystage "
            + "URL = 's3://mybucket/mypath/' "
            + "credentials = (aws_key_id = '****' "
            + "aws_secret_key = '****'); "
            + "create stage mystage2 "
            + "URL = 'azure//mystorage.blob.core.windows.net/cont' "
            + "credentials = (azure_sas_token = "
            + "'?sv=2016-05-31&ss=b&srt=sco&sp=rwdl&se=2018-06-27T10:05:50Z&"
            + "st=2017-06-27T02:05:50Z&spr=https,http&"
            + "sig=****')";

    String masked = SecretDetector.maskSecrets(sqlText);
    System.out.println(masked);
    System.out.println(maskedSqlText);
    assertThat(
        "Text with AWS secret and Azure SAS token is not masked", maskedSqlText.equals(masked));

    String randomString = RandomStringUtils.random(500);
    assertThat(
        "Text without secrets is not unmodified",
        randomString.equals(SecretDetector.maskSecrets(randomString)));
  }

  @Test
  public void testMaskPasswordFromConnectionString() {
    // Since we have `&` in password regex pattern, we will have false positive masking here
    String connectionStr =
        "\"jdbc:snowflake://xxx.snowflakecomputing" + ".com/?user=xxx&password=xxxxxx&role=xxx\"";
    String maskedConnectionStr =
        "\"jdbc:snowflake://xxx.snowflakecomputing" + ".com/?user=xxx&password=**** ";
    assertThat(
        "Text with password is not masked",
        maskedConnectionStr.equals(SecretDetector.maskSecrets(connectionStr)));

    connectionStr = "jdbc:snowflake://xxx.snowflakecomputing" + ".com/?user=xxx&password=xxxxxx";
    maskedConnectionStr =
        "jdbc:snowflake://xxx.snowflakecomputing" + ".com/?user=xxx&password=**** ";
    assertThat(
        "Text with password is not masked",
        maskedConnectionStr.equals(SecretDetector.maskSecrets(connectionStr)));

    connectionStr = "jdbc:snowflake://xxx.snowflakecomputing" + ".com/?user=xxx&passcode=xxxxxx";
    maskedConnectionStr =
        "jdbc:snowflake://xxx.snowflakecomputing" + ".com/?user=xxx&passcode=**** ";
    assertThat(
        "Text with password is not masked",
        maskedConnectionStr.equals(SecretDetector.maskSecrets(connectionStr)));

    connectionStr = "jdbc:snowflake://xxx.snowflakecomputing" + ".com/?user=xxx&passWord=xxxxxx";
    maskedConnectionStr =
        "jdbc:snowflake://xxx.snowflakecomputing" + ".com/?user=xxx&passWord=**** ";
    assertThat(
        "Text with password is not masked",
        maskedConnectionStr.equals(SecretDetector.maskSecrets(connectionStr)));
  }

  @Test
  public void sasTokenFilterTest() throws Exception {
    String messageText = "\"privateKeyData\": \"aslkjdflasjf\"";

    String filteredMessageText = "\"privateKeyData\": \"XXXX\"";

    String result = SecretDetector.maskSecrets(messageText);

    assertEquals(filteredMessageText, result);
  }

  @Test
  public void testMaskParameterValue() {
    Map<String, String> testParametersMasked = new HashMap<>();
    testParametersMasked.put("passcodeInPassword", "test");
    testParametersMasked.put("passcode", "test");
    testParametersMasked.put("id_token", "test");
    testParametersMasked.put("private_key_pwd", "test");
    testParametersMasked.put("proxyPassword", "test");
    testParametersMasked.put("proxyUser", "test");
    testParametersMasked.put("privatekey", "test");
    testParametersMasked.put("private_key_base64", "test");
    testParametersMasked.put("privateKeyBase64", "test");
    testParametersMasked.put("id_token_password", "test");
    testParametersMasked.put("masterToken", "test");
    testParametersMasked.put("mfaToken", "test");
    testParametersMasked.put("password", "test");
    testParametersMasked.put("sessionToken", "test");
    testParametersMasked.put("token", "test");
    testParametersMasked.put("oauthClientId", "test");
    testParametersMasked.put("oauthClientSecret", "test");

    Map<String, String> testParametersUnmasked = new HashMap<>();
    testParametersUnmasked.put("oktausername", "test");
    testParametersUnmasked.put("authenticator", "test");
    testParametersUnmasked.put("proxyHost", "test");
    testParametersUnmasked.put("user", "test");
    testParametersUnmasked.put("private_key_file", "test");

    for (Map.Entry<String, String> entry : testParametersMasked.entrySet()) {
      assertEquals("****", SecretDetector.maskParameterValue(entry.getKey(), entry.getValue()));
    }
    for (Map.Entry<String, String> entry : testParametersUnmasked.entrySet()) {
      assertEquals("test", SecretDetector.maskParameterValue(entry.getKey(), entry.getValue()));
    }
  }

  @Test
  public void testMaskconnectionToken() {
    String connectionToken = "\"Authorization: Snowflake Token=\"XXXXXXXXXX\"\"";

    String maskedConnectionToken = "\"Authorization: Snowflake Token=\"****\"\"";

    assertThat(
        "Text with connection token is not masked",
        maskedConnectionToken.equals(SecretDetector.maskSecrets(connectionToken)));

    connectionToken = "\"{\"requestType\":\"ISSUE\",\"idToken\":\"XXXXXXXX\"}\"";

    maskedConnectionToken = "\"{\"requestType\":\"ISSUE\",\"idToken\":\"****\"}\"";

    assertThat(
        "Text with connection token is not masked",
        maskedConnectionToken.equals(SecretDetector.maskSecrets(connectionToken)));
  }

  @Test
  public void testMaskOAuthSecrets() {
    String tokenResponseJson =
        "{\n"
            + "  \"access_token\" : \"some:FAKE_token123\",\n"
            + "  \"refresh_token\" : \"some:FAKE_token123\",\n"
            + "  \"token_type\" : \"Bearer\",\n"
            + "  \"username\" : \"OAUTH_TEST_AUTH_CODE\",\n"
            + "  \"scope\" : \"refresh_token session:role:ANALYST\",\n"
            + "  \"expires_in\" : 600,\n"
            + "  \"refresh_token_expires_in\" : 86399,\n"
            + "  \"idpInitiated\" : false\n"
            + "}";

    String maskedTokenResponseJson =
        "{\n"
            + "  \"access_token\":\"****\",\n"
            + "  \"refresh_token\":\"****\",\n"
            + "  \"token_type\" : \"Bearer\",\n"
            + "  \"username\" : \"OAUTH_TEST_AUTH_CODE\",\n"
            // The scope value follows a "token" label and now contains characters the token value
            // class accepts, so it is redacted along with real token values. Over-masking a scope
            // string costs some log detail; under-masking a session token does not have an
            // equivalent cheap remedy.
            + "  \"scope\" : \"refresh_token ****\",\n"
            + "  \"expires_in\" : 600,\n"
            + "  \"refresh_token_expires_in\" : 86399,\n"
            + "  \"idpInitiated\" : false\n"
            + "}";

    assertThat(
        "Text with OAuth tokens is not masked",
        maskedTokenResponseJson.equals(SecretDetector.maskSecrets(tokenResponseJson)));
  }

  private JSONObject generateJsonObject() {
    // a base json object
    JSONObject obj = new JSONObject();
    obj.put("hello", "world");
    obj.put("number", 256);
    obj.put("boolean", true);
    return obj;
  }

  @Test
  public void testMaskJsonObject() {
    final String connStr =
        "https://snowflake.fakehostname.local:fakeport?LOGINTIMEOUT=20&ACCOUNT=fakeaccount&PASSWORD=fakepassword&USER=fakeuser";
    final String maskedConnStr =
        "https://snowflake.fakehostname.local:fakeport?LOGINTIMEOUT=20&ACCOUNT=fakeaccount&PASSWORD=**** ";

    JSONObject obj = generateJsonObject();
    obj.put("connStr", connStr);
    JSONObject maskedObj = generateJsonObject();
    maskedObj.put("connStr", maskedConnStr);

    assertThat(
        "JSONObject is not masked successfully",
        maskedObj.equals(SecretDetector.maskJsonObject(obj)));

    obj.put("connStr", connStr);
    JSONObject nestedObj = generateJsonObject();
    nestedObj.put("subJson", obj);
    JSONObject maskedNestedObj = generateJsonObject();
    maskedNestedObj.put("subJson", maskedObj);

    assertThat(
        "nested JSONObject is not masked successfully",
        maskedNestedObj.equals(SecretDetector.maskJsonObject(nestedObj)));
  }

  @Test
  public void testMaskJsonArray() {
    final String connStr =
        "https://snowflake.fakehostname.local:fakeport?LOGINTIMEOUT=20&ACCOUNT=fakeaccount&PASSWORD=fakepassword&USER=fakeuser";
    final String maskedConnStr =
        "https://snowflake.fakehostname.local:fakeport?LOGINTIMEOUT=20&ACCOUNT=fakeaccount&PASSWORD=**** ";
    final String pwdStr = "password=ThisShouldBeMasked";
    final String maskedPwdStr = "password=****";

    JSONObject obj = generateJsonObject();
    obj.put("connStr", connStr);

    JSONObject maskedObj = generateJsonObject();
    maskedObj.put("connStr", maskedConnStr);

    JSONArray array = new JSONArray();
    array.add(obj);
    array.add(pwdStr);
    JSONArray maskedArray = new JSONArray();
    maskedArray.add(maskedObj);
    maskedArray.add(maskedPwdStr);

    assertThat(
        "jsonArray is not masked successfully",
        maskedArray.equals(SecretDetector.maskJsonArray(array)));

    JSONObject obj1 = generateJsonObject();
    obj1.put("connStr", connStr);

    JSONObject maskedObj1 = generateJsonObject();
    maskedObj1.put("connStr", maskedConnStr);

    JSONArray array1 = new JSONArray();
    array1.add(obj1);
    array1.add(pwdStr);
    JSONArray maskedArray1 = new JSONArray();
    maskedArray1.add(maskedObj1);
    maskedArray1.add(maskedPwdStr);

    JSONObject nestedObjArray = generateJsonObject();
    nestedObjArray.put("array", array1);
    JSONObject maskedNestedObjArray = generateJsonObject();
    maskedNestedObjArray.put("array", maskedArray1);

    assertThat(
        "JSONArray within JSONObject is not masked successfully",
        maskedNestedObjArray.equals(SecretDetector.maskJsonObject(nestedObjArray)));
  }

  @Test
  public void testMaskJacksonObject() {
    final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    ObjectNode objNode = mapper.createObjectNode();
    objNode.put("testInfo", "pwd=HelloWorld!");

    String maskedObjNodeStr = "{\"testInfo\":\"pwd=**** \"}";
    assertThat(
        "Jackson ObjectNode is not masked successfully",
        maskedObjNodeStr.equals(SecretDetector.maskJacksonNode(objNode).toString()));

    // test nested object node
    ObjectNode objNode1 = mapper.createObjectNode();
    objNode1.put("testInfo", "pwd=HelloWorld!");
    ObjectNode objNodeNested = mapper.createObjectNode();
    objNodeNested.put("dummy", "dummy");
    objNodeNested.put("testInfo2", "sig=ajwAWD45%+ajrH92knKfsfakj");
    objNode1.set("testNested", objNodeNested);

    String maskedObjNestedStr =
        "{\"testInfo\":\"pwd=**** \",\"testNested\":{\"dummy\":\"dummy\",\"testInfo2\":\"sig=****\"}}";
    assertThat(
        "Nested Jackson ObjectNode is not masked successfully",
        maskedObjNestedStr.equals(SecretDetector.maskJacksonNode(objNode1).toString()));

    // test array node
    ObjectNode objNode2 = mapper.createObjectNode();
    objNode2.put("testInfo", "aws_secret_key= 'fakeAWSsecretKey!123'");
    ArrayNode arrayObj = mapper.createArrayNode();
    arrayObj.add(objNode2);
    arrayObj.add("fake token: SDFADAVN4ASD28ASFG3234x");

    String maskedArrayNodeStr = "[{\"testInfo\":\"aws_secret_key= '****'\"},\"fake token: ****\"]";
    assertThat(
        "Jackson ArrayNode is not masked successfully",
        maskedArrayNodeStr.equals(SecretDetector.maskJacksonNode(arrayObj).toString()));

    // test nested array node
    ObjectNode objNode3 = mapper.createObjectNode();
    objNode3.put("testInfo", "\"privateKeyData\": \"abcdefg012345/=+\"");
    ArrayNode arrayObj1 = mapper.createArrayNode();
    arrayObj1.add(objNode3);
    ObjectNode objNode4 = mapper.createObjectNode();
    objNode4.set("testArrayNode", arrayObj1);
    objNode4.put("hello", "world");
    objNode4.put("password", "password = HelloWorld!");

    String maskedNestedArrayStr =
        "{\"testArrayNode\":[{\"testInfo\":\"\\\"privateKeyData\\\": \\\"XXXX\\\"\"}],\"hello\":\"world\",\"password\":\"password = **** \"}";
    SecretDetector.maskJacksonNode(objNode4);
    assertThat(
        "Nested Jackson array node is not masked successfully",
        maskedNestedArrayStr.equals(SecretDetector.maskJacksonNode(objNode4).toString()));
  }

  @Test
  public void testEncryptionMaterialFilter() throws Exception {
    String messageText =
        "{\"data\":"
            + "{\"autoCompress\":true,"
            + "\"overwrite\":false,"
            + "\"clientShowEncryptionParameter\":true,"
            + "\"encryptionMaterial\":{\"queryStageMasterKey\":\"asdfasdfasdfasdf==\",\"queryId\":\"01b6f5ba-0002-0181-0000-11111111da\",\"smkId\":1111},"
            + "\"stageInfo\":{\"locationType\":\"AZURE\", \"region\":\"eastus2\"}";

    String filteredMessageText =
        "{\"data\":"
            + "{\"autoCompress\":true,"
            + "\"overwrite\":false,"
            + "\"clientShowEncryptionParameter\":true,"
            + "\"encryptionMaterial\" : ****,"
            + "\"stageInfo\":{\"locationType\":\"AZURE\", \"region\":\"eastus2\"}";

    String result = SecretDetector.filterEncryptionMaterial(messageText);

    assertEquals(filteredMessageText, result);
  }

  @Test
  public void testMaskQrmk() {
    // A qrmk-labelled value is masked.
    final String qrmk = "3dOoaBhkBOv1L15q1QYdz6z5Rzg1qEQTsm2ANFcaZ8I="; // pragma: allowlist secret
    final String message = "qrmk: " + qrmk;

    final String masked = SecretDetector.maskSecrets(message);

    assertThat("qrmk value should not be present after masking", !masked.contains(qrmk));
    assertThat("qrmk redaction marker should be present", masked.contains("****"));
    assertEquals("qrmk: ****", masked);
  }

  @Test
  public void testMaskSigV4Params() {
    // X-Amz-Credential / X-Amz-Security-Token are masked (X-Amz-Signature via SAS masking).
    // pragma: allowlist nextline secret
    final String credential = "AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request";
    // pragma: allowlist nextline secret
    final String securityToken = "FQoGZXIvYXdzEBYaDHRlc3RzZWN1cml0eT1234567890abcdefGHIJKLMNOP%3D";
    final String signature = "zFiRkdB9RtRRYomppVes4fQ%2ByWw%3D"; // pragma: allowlist secret
    final String url =
        "https://bucket.s3.amazonaws.com/path?"
            + "X-Amz-Credential="
            + credential
            + "&X-Amz-Security-Token="
            + securityToken
            + "&X-Amz-Signature="
            + signature;

    final String masked = SecretDetector.maskSecrets(url);

    assertThat(
        "X-Amz-Credential value should not be present after masking", !masked.contains(credential));
    assertThat(
        "X-Amz-Security-Token value should not be present after masking",
        !masked.contains(securityToken));
    assertThat(
        "X-Amz-Signature value should not be present after masking", !masked.contains(signature));
    assertThat("redaction marker should be present", masked.contains("****"));
    assertEquals(
        "https://bucket.s3.amazonaws.com/path?"
            + "X-Amz-Credential=****&X-Amz-Security-Token=****&X-Amz-Signature=****",
        masked);
  }

  /**
   * Every Snowflake session token carries a "ver:&lt;n&gt;-" prefix, and the V2/V4 formats carry a
   * second colon in their "-did:" segment, so the token value class has to accept ':'. Cover all
   * four minted formats in the label shapes that show up in logs.
   */
  @Test
  public void testMaskSessionTokenOfEveryVersion() {
    // Placeholder standing in for the encrypted portion of a real token.
    final String encrypted = "AAAABBBBCCCCDDDDEEEEFFFF";
    final String[] tokens = {
      "ver:1-hint:1234-" + encrypted,
      "ver:2-hint:1234-did:5678-" + encrypted,
      "ver:3-hint:1234-" + encrypted,
      "ver:4-hint:1234-did:5678-" + encrypted,
    };

    for (String token : tokens) {
      // quoted, as it appears in a JSON response body
      assertEquals("Token=\"****\"", SecretDetector.maskSecrets("Token=\"" + token + "\""));
      // bare, as it appears in a query string or a connection property dump
      assertEquals("token=****", SecretDetector.maskSecrets("token=" + token));
      // colon separated, as it appears in a header dump
      assertEquals("token: ****", SecretDetector.maskSecrets("token: " + token));

      assertThat(
          "token value should not survive masking in any shape",
          !SecretDetector.maskSecrets("Token=\"" + token + "\"").contains(encrypted)
              && !SecretDetector.maskSecrets("token=" + token).contains(encrypted)
              && !SecretDetector.maskSecrets("token: " + token).contains(encrypted));
    }
  }

  /**
   * SessionUtil logs the whole login response body when the response is unsuccessful. Both the
   * session token and the master token have to be gone by the time it reaches the log.
   */
  @Test
  public void testMaskLoginResponseBody() {
    final String sessionToken = "ver:3-hint:1234-SESSIONAAAABBBBCCCC";
    final String masterToken = "ver:3-hint:1234-MASTERDDDDEEEEFFFF";
    final String responseBody =
        "{\"data\":{"
            + "\"masterToken\":\""
            + masterToken
            + "\","
            + "\"token\":\""
            + sessionToken
            + "\","
            + "\"validityInSeconds\":3600,"
            + "\"masterValidityInSeconds\":14400,"
            + "\"displayUserName\":\"TESTUSER\","
            + "\"serverVersion\":\"8.0.0\","
            + "\"firstLogin\":false,"
            + "\"remMeToken\":null,"
            + "\"remMeValidityInSeconds\":0,"
            + "\"healthCheckInterval\":45,"
            + "\"sessionId\":1234567890,"
            + "\"idToken\":null,"
            + "\"idTokenValidityInSeconds\":0,"
            + "\"sessionInfo\":{\"databaseName\":\"TESTDB\",\"schemaName\":\"PUBLIC\","
            + "\"warehouseName\":\"TESTWH\",\"roleName\":\"ANALYST\"}"
            + "},\"code\":null,\"message\":null,\"success\":true}";

    final String masked = SecretDetector.maskSecrets(responseBody);

    assertThat("session token should not survive masking", !masked.contains(sessionToken));
    assertThat("master token should not survive masking", !masked.contains(masterToken));
    assertThat("session token should be redacted", masked.contains("\"token\":\"****\""));
    assertThat("master token should be redacted", masked.contains("\"masterToken\":\"****\""));
    // The rest of the body stays readable.
    assertThat("non secret fields should be kept", masked.contains("\"success\":true"));
    assertThat("non secret fields should be kept", masked.contains("\"sessionId\":1234567890"));
  }

  /**
   * The chunk encryption key travels both as a qrmk-labelled value and as the SSE-C customer key
   * header. Cover the header-name form in each separator shape the pattern supports.
   */
  @Test
  public void testMaskSseCustomerKeyHeader() {
    // pragma: allowlist nextline secret
    final String customerKey = "3dOoaBhkBOv1L15q1QYdz6z5Rzg1qEQTsm2ANFcaZ8I=";
    final String header = "x-amz-server-side-encryption-customer-key";

    assertEquals(header + ": ****", SecretDetector.maskSecrets(header + ": " + customerKey));
    assertEquals(header + "=****", SecretDetector.maskSecrets(header + "=" + customerKey));
    assertEquals(
        "Adding header name: " + header + ", value: ****",
        SecretDetector.maskSecrets("Adding header name: " + header + ", value: " + customerKey));
    assertEquals(
        "\"" + header + "\":\"****\"",
        SecretDetector.maskSecrets("\"" + header + "\":\"" + customerKey + "\""));

    assertThat(
        "customer key should not survive masking in any shape",
        !SecretDetector.maskSecrets(header + ": " + customerKey).contains(customerKey)
            && !SecretDetector.maskSecrets(header + "=" + customerKey).contains(customerKey)
            && !SecretDetector.maskSecrets(
                    "Adding header name: " + header + ", value: " + customerKey)
                .contains(customerKey));

    // A list of header names on its own carries no value to redact.
    final String headerNamesOnly = "status: 200 OK, header names: [" + header + ", Content-Length]";
    assertEquals(headerNamesOnly, SecretDetector.maskSecrets(headerNamesOnly));
  }
}
