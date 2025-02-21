package net.snowflake.client.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@DisabledOnOs(OS.WINDOWS)
public @interface DontRunOnWindows {}
