/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@EnabledOnOs({OS.LINUX, OS.AIX})
public @interface RunOnLinux {}
