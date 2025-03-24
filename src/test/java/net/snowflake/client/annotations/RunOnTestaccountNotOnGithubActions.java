package net.snowflake.client.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@EnabledIfEnvironmentVariable(named = "SNOWFLAKE_TEST_ACCOUNT", matches = "testaccount")
@DisabledIfEnvironmentVariable(named = "GITHUB_ACTIONS", matches = ".*")
public @interface RunOnTestaccountNotOnGithubActions {}
