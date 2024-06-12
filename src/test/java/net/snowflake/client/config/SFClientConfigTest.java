package net.snowflake.client.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SFClientConfigTest {
  @Test
  public void testSFConfigOptions() {
    SFClientConfig.CommonProps props = new SFClientConfig.CommonProps();
    props.setLogLevel("info");
    props.setLogPath("jdbc_log");
    SFClientConfig sfClientConfig = new SFClientConfig();
    SFClientConfig sfClientConfig1 = new SFClientConfig(props);

    assertTrue(sfClientConfig1.equals(sfClientConfig1));
    assertFalse(sfClientConfig.equals(sfClientConfig1));

    SFClientConfig.CommonProps props2 = new SFClientConfig.CommonProps();
    props2.CommonProps("info", "jdbc_log");
    sfClientConfig.setCommonProps(props2);

    assertTrue(sfClientConfig.equals(sfClientConfig1));
    assertEquals(sfClientConfig.hashCode(), sfClientConfig1.hashCode());
  }

  @Test
  public void testCommonPropsAttr() {
    SFClientConfig.CommonProps commonProps = new SFClientConfig.CommonProps();
    SFClientConfig.CommonProps commonProps2 = new SFClientConfig.CommonProps();
    commonProps.CommonProps("info", "jdbc_log");

    assertTrue(commonProps.equals(commonProps));
    assertFalse(commonProps.equals(commonProps2));

    commonProps2.setLogLevel("info");
    commonProps2.setLogPath("jdbc_log");

    assertTrue(commonProps.equals(commonProps2));
  }
}
