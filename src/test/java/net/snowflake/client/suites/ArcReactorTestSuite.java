package net.snowflake.client.suites;

import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.DisplayName;
import org.junit.platform.suite.api.IncludeTags;

/**
 * Tests that need Arc Reactor / Top Secret region JVM configuration supplied at launch, which
 * cannot be set from within a test: {@code jdk.tls.client.protocols} (and future additions) is read
 * at the first JSSE use and cached, so {@code System.setProperty} in a test is unreliable.
 *
 * <p>Run with {@code mvn test -DarcReactorTests}; the flags come from the {@code
 * arcReactor.jvmFlags} property in that profile.
 */
@BaseTestSuite
@DisplayName("Arc Reactor tests")
@IncludeTags(TestTags.ARC_REACTOR)
public class ArcReactorTestSuite {}
