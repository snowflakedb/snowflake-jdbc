package net.snowflake.client;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@DisabledIfEnvironmentVariable(named = "CLOUD_PROVIDER", matches = "(?i)GCP(?-i)")
public @interface DontRunOnGCP {}