/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.Assert.assertEquals;

import com.microsoft.azure.storage.StorageExtendedErrorInformation;
import java.util.LinkedHashMap;
import org.junit.Test;

public class SnowflakeAzureClientTest {
  @Test
  public void testFormatStorageExtendedErrorInformation() {
    String expectedStr0 =
        "StorageExceptionExtendedErrorInformation: {ErrorCode= 403, ErrorMessage= Server refuses"
            + " to authorize the request, AdditionalDetails= {}}";
    String expectedStr1 =
        "StorageExceptionExtendedErrorInformation: {ErrorCode= 403, ErrorMessage= Server refuses"
            + " to authorize the request, AdditionalDetails= { key1= helloworld,key2= ,key3="
            + " fakemessage}}";
    StorageExtendedErrorInformation info = new StorageExtendedErrorInformation();
    info.setErrorCode("403");
    info.setErrorMessage("Server refuses to authorize the request");
    String formatedStr = SnowflakeAzureClient.FormatStorageExtendedErrorInformation(info);
    assertEquals(expectedStr0, formatedStr);

    LinkedHashMap<String, String[]> map = new LinkedHashMap<>();
    map.put("key1", new String[] {"hello", "world"});
    map.put("key2", new String[] {});
    map.put("key3", new String[] {"fake", "message"});
    info.setAdditionalDetails(map);
    formatedStr = SnowflakeAzureClient.FormatStorageExtendedErrorInformation(info);
    assertEquals(expectedStr1, formatedStr);
  }
}
