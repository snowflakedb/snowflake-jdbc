package net.snowflake.client.suites;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.platform.suite.api.ExcludePackages;
import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Suite
@SuiteDisplayName("Testowanie")
@SelectPackages("net.snowflake.client")
@ExcludePackages("net.snowflake.client.suites")
@IncludeClassNamePatterns(".+")
public @interface BaseTestSuite {}
