package net.snowflake.client.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.OS;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@EnabledIfEnvironmentVariable(named = "GITHUB_ACTIONS", matches = ".*")
@DisabledOnOs(OS.MAC)
public @interface RunOnGithubActionsNotMac {}
