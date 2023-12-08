package net.snowflake.client;

import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
@SfTest(sinceVersion = "5.6.7")
public class TestSinceConditionRuleTest {
  private static final Statement normalRun = mock(Statement.class);
  private static final Statement ignoreRun = mock(Statement.class);

  @Rule
  public final TestSinceConditionRule testSinceConditionRule = new TestSinceConditionRule(TestSinceConditionRule.Version.parse("2.3.4"));

  @Rule
  public final TestWatcher testWatcher = new TestWatcher() {
    @Override
    protected void succeeded(Description description) {
      succeededTests.add(description.getMethodName());
    }
  };

  private static List<String> succeededTests = new ArrayList<>();

  @AfterClass
  public static void afterAll() {
    assertThat(succeededTests, hasItem("testDecidingIfTestShouldBeRun[0]"));
  }

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][]{
        {TestSinceConditionRule.Version.parse("1.2.3"), true, normalRun},
        {TestSinceConditionRule.Version.parse("2.3.3"), true, normalRun},
        {TestSinceConditionRule.Version.parse("2.3.3"), true, normalRun},
        {TestSinceConditionRule.Version.parse("2.3.4"), false, normalRun},
        {TestSinceConditionRule.Version.parse("2.3.4"), true, ignoreRun},
        {TestSinceConditionRule.Version.parse("2.3.5"), true, ignoreRun},
        {TestSinceConditionRule.Version.parse("2.3.5"), false, ignoreRun},
        {TestSinceConditionRule.Version.parse("3.0.0"), true, ignoreRun},
        {TestSinceConditionRule.Version.parse("3.0.0"), false, ignoreRun},
    };
  }

  private final TestSinceConditionRule.Version version;
  private final boolean oldDriverTest;
  private final Statement result;

  public TestSinceConditionRuleTest(TestSinceConditionRule.Version version, boolean oldDriverTest, Statement result) {
    this.version = version;
    this.oldDriverTest = oldDriverTest;
    this.result = result;
  }

  @Test
  @SfTest(sinceVersion = "1.0.0")
  public void testDecidingIfTestShouldBeRun() {
    assertEquals(result, testSinceConditionRule.decideIfShouldBeRun(normalRun, ignoreRun, version, oldDriverTest));
  }

  @SfTest(sinceVersion = "1000.1000.1000")
  @Test
  public void testShouldNeverStart() {
    throw new AssertionError("should never start this test");
  }

  @Test
  public void testShouldNeverStartBecauseClassVersionIsTooHigh() {
    throw new AssertionError("should never start this test");
  }
}