package net.snowflake.client.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@DisabledOnJre(JRE.JAVA_8)
public @interface DontRunOnJava8 {}
