/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.BindUploaderIT.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.TimeZone;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.bind.BindUploader;
import org.junit.*;
import org.junit.experimental.categories.Category;

/**
 * Bind Uploader tests for the latest JDBC driver. This doesn't work for the oldest supported
 * driver. Revisit this tests whenever bumping up the oldest supported driver to examine if the
 * tests still is not applicable. If it is applicable, move tests to BindUploaderIT so that both the
 * latest and oldest supported driver run the tests.
 */
@Category(TestCategoryOthers.class)
public class BindUploaderLatestIT extends BaseJDBCTest {
  BindUploader bindUploader;
  Connection conn;
  SFSession session;
  TimeZone prevTimeZone; // store last time zone and restore after tests

  @BeforeClass
  public static void classSetUp() throws Exception {
    BindUploaderIT.classSetUp();
  }

  @AfterClass
  public static void classTearDown() throws Exception {
    BindUploaderIT.classTearDown();
  }

  @Before
  public void setUp() throws Exception {
    conn = getConnection();
    session = conn.unwrap(SnowflakeConnectionV1.class).getSfSession();
    bindUploader = BindUploader.newInstance(session, STAGE_DIR);
    prevTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  @After
  public void tearDown() throws SQLException {
    conn.close();
    bindUploader.close();
    TimeZone.setDefault(prevTimeZone);
  }
}
