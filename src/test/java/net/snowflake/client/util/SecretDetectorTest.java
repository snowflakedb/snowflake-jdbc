package net.snowflake.client.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class SecretDetectorTest
{
  @Test
  public void testMaskAWSSecret()
  {
    String sql = "copy into 's3://xxxx/test' from \n" +
                 "(select seq1(), random()\n" +
                 ", random(), random(), random(), random()\n" +
                 ", random(), random(), random(), random()\n" +
                 ", random() , random(), random(), random()\n" +
                 "\tfrom table(generator(rowcount => 10000)))\n" +
                 "credentials=(\n" +
                 "  aws_key_id='xxdsdfsafds'\n" +
                 "  aws_secret_key='safas+asfsad+safasf'\n" +
                 "  )\n" +
                 "OVERWRITE = TRUE \n" +
                 "MAX_FILE_SIZE = 500000000 \n" +
                 "HEADER = TRUE \n" +
                 "FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE )\n" +
                 ";";
    String correct = "copy into 's3://xxxx/test' from \n" +
                     "(select seq1(), random()\n" +
                     ", random(), random(), random(), random()\n" +
                     ", random(), random(), random(), random()\n" +
                     ", random() , random(), random(), random()\n" +
                     "\tfrom table(generator(rowcount => 10000)))\n" +
                     "credentials=(\n" +
                     "  aws_key_id='☺☺☺☺☺☺☺☺☺☺☺'\n" +
                     "  aws_secret_key='☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺'\n" +
                     "  )\n" +
                     "OVERWRITE = TRUE \n" +
                     "MAX_FILE_SIZE = 500000000 \n" +
                     "HEADER = TRUE \n" +
                     "FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE )\n" +
                     ";";
    String masked = SecretDetector.maskAWSSecret(sql);
    assertThat("secret masked", correct.compareTo(masked) == 0);
  }

  @Test
  public void testMaskSASToken()
  {
    // Initializing constants
    final String azureSasToken =
        "https://someaccounts.blob.core.windows.net/results/018b90ab-0033-" +
        "5f8e-0000-14f1000bd376_0/main/data_0_0_1?sv=2015-07-08&amp;" +
        "sig=iCvQmdZngZNW%2F4vw43j6%2BVz6fndHF5LI639QJba4r8o%3D&amp;" +
        "spr=https&amp;st=2016-04-12T03%3A24%3A31Z&amp;" +
        "se=2016-04-13T03%3A29%3A31Z&amp;srt=s&amp;ss=bf&amp;sp=rwl";

    final String maskedAzureSasToken =
        "https://someaccounts.blob.core.windows.net/results/018b90ab-0033-" +
        "5f8e-0000-14f1000bd376_0/main/data_0_0_1?sv=2015-07-08&amp;" +
        "sig=☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺&amp;" +
        "spr=https&amp;st=2016-04-12T03%3A24%3A31Z&amp;" +
        "se=2016-04-13T03%3A29%3A31Z&amp;srt=s&amp;ss=bf&amp;sp=rwl";

    final String s3SasToken =
        "https://somebucket.s3.amazonaws.com/vzy1-s-va_demo0/results/018b92f3" +
        "-01c2-02dd-0000-03d5000c8066_0/main/data_0_0_1?" +
        "x-amz-server-side-encryption-customer-algorithm=AES256&" +
        "response-content-encoding=gzip&AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE" +
        "&Expires=1555481960&Signature=zFiRkdB9RtRRYomppVes4fQ%2ByWw%3D";

    final String maskedS3SasToken =
        "https://somebucket.s3.amazonaws.com/vzy1-s-va_demo0/results/018b92f3" +
        "-01c2-02dd-0000-03d5000c8066_0/main/data_0_0_1?" +
        "x-amz-server-side-encryption-customer-algorithm=AES256&" +
        "response-content-encoding=gzip&AWSAccessKeyId=☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺" +
        "&Expires=1555481960&Signature=☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺";

    assertThat("Azure SAS token is not masked",
               maskedAzureSasToken.equals(
                   SecretDetector.maskSASToken(azureSasToken)));

    assertThat("S3 SAS token is not masked",
               maskedS3SasToken.equals(SecretDetector.maskSASToken(s3SasToken)));

    String randomString = RandomStringUtils.random(200);
    assertThat("Text without secrets is not unmodified",
               randomString.equals(SecretDetector.maskSASToken(randomString)));

    assertThat("Text with 2 Azure SAS tokens is not masked",
               (maskedAzureSasToken + maskedAzureSasToken).equals(
                   SecretDetector.maskSASToken(azureSasToken + azureSasToken)));

    assertThat("Text with 2 S3 SAS tokens is not masked",
               (maskedAzureSasToken + maskedAzureSasToken).equals(
                   SecretDetector.maskSASToken(azureSasToken + azureSasToken)));

    assertThat("Text with Azure and S3 SAS tokens is not masked",
               (maskedAzureSasToken + maskedS3SasToken).equals(
                   SecretDetector.maskSASToken(azureSasToken + s3SasToken)));
  }

  @Test
  public void testMaskSecrets()
  {
    // Text containing AWS secret and Azure SAS token
    final String sqlText =
        "create stage mystage " +
        "URL = 's3://mybucket/mypath/' " +
        "credentials = (aws_key_id = 'AKIAIOSFODNN7EXAMPLE' " +
        "aws_secret_key = 'frJIUN8DYpKDtOLCwo//yllqDzg='); " +
        "create stage mystage2 " +
        "URL = 'azure//mystorage.blob.core.windows.net/cont' " +
        "credentials = (azure_sas_token = " +
        "'?sv=2016-05-31&ss=b&srt=sco&sp=rwdl&se=2018-06-27T10:05:50Z&" +
        "st=2017-06-27T02:05:50Z&spr=https,http&" +
        "sig=bgqQwoXwxzuD2GJfagRg7VOS8hzNr3QLT7rhS8OFRLQ%3D')";

    final String maskedSqlText =
        "create stage mystage " +
        "URL = 's3://mybucket/mypath/' " +
        "credentials = (aws_key_id = '☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺' " +
        "aws_secret_key = '☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺'); " +
        "create stage mystage2 " +
        "URL = 'azure//mystorage.blob.core.windows.net/cont' " +
        "credentials = (azure_sas_token = " +
        "'?sv=2016-05-31&ss=b&srt=sco&sp=rwdl&se=2018-06-27T10:05:50Z&" +
        "st=2017-06-27T02:05:50Z&spr=https,http&" +
        "sig=☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺')";

    assertThat("Text with AWS secret and Azure SAS token is not masked",
               maskedSqlText.equals(SecretDetector.maskSecrets(sqlText)));

    String randomString = RandomStringUtils.random(500);
    assertThat("Text without secrets is not unmodified",
               randomString.equals(SecretDetector.maskSecrets(randomString)));
  }
}
