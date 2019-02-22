package net.snowflake.client.util;

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
    assertThat("secret masked", correct.compareTo(masked)==0);
  }
}
