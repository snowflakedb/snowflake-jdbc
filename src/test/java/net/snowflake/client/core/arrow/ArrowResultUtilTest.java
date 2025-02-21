package net.snowflake.client.core.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.stream.Stream;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.providers.TimezoneProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class ArrowResultUtilTest {
  @AfterAll
  public static void clearTimeZone() {
    System.clearProperty("user.timezone");
  }

  public static void setTimeZone(String string) {
    System.setProperty("user.timezone", string);
  }

  @ParameterizedTest(name = "Timezone = {0}")
  @ArgumentsSource(TimezoneProvider.class)
  @Disabled
  /** This is to show we can have 30X improvement using new API */
  public void testGetDatePerformance(String timezone) throws SFException {
    setTimeZone(timezone);
    Random random = new Random();
    int dateBound = 50000;
    int times = 100000;
    SFSession session = new SFSession();
    long start = System.currentTimeMillis();
    TimeZone tz = TimeZone.getDefault();
    int[] days = new int[times];
    for (int i = 0; i < times; i++) {
      days[i] = random.nextInt(dateBound) - dateBound / 2;
    }

    for (int i = 0; i < times; i++) {
      ResultUtil.getDate(Integer.toString(days[i]), tz, session);
    }
    long duration1 = System.currentTimeMillis() - start;

    start = System.currentTimeMillis();
    for (int i = 0; i < times; i++) {
      ArrowResultUtil.getDate(days[i], tz, tz);
    }
    long duration2 = System.currentTimeMillis() - start;

    start = System.currentTimeMillis();
    for (int i = 0; i < times; i++) {
      ArrowResultUtil.getDate(days[i]);
    }
    long duration3 = System.currentTimeMillis() - start;
    System.out.println(duration1 + " " + duration2 + " " + duration3);
  }

  private static class testCasesProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      List<String> timezones =
          new ArrayList<String>() {
            {
              add("UTC");
              add("America/Los_Angeles");
              add("America/New_York");
              add("Asia/Singapore");
              add("MEZ");
            }
          };

      long[] cases = {-1123456789, -123456789, 123456789, 123123456789L, -123123456789L};
      long[] millisecs = {-1124, -124, 123, 123123, -123124};
      int[] nanos = {876543211, 876543211, 123456789, 123456789, 876543211};

      List<Arguments> args = new ArrayList<>();
      for (String timezone : timezones) {
        for (int i = 0; i < cases.length; i++) {
          args.add(Arguments.of(timezone, cases[i], millisecs[i], nanos[i]));
        }
      }

      return args.stream();
    }
  }

  @ParameterizedTest
  @ArgumentsSource(testCasesProvider.class)
  public void testToJavaTimestamp(String timezone, long cas, long millisecs, int nanos) {
    // ex: -1.123456789, -0.123456789, 0.123456789, 123.123456789, -123.123456789
    setTimeZone(timezone);
    int scale = 9;
    Timestamp ts = ArrowResultUtil.toJavaTimestamp(cas, scale);
    assertEquals(millisecs, ts.getTime());
    assertEquals(nanos, ts.getNanos());
  }
}
