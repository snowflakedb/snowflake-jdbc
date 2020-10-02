/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.BindUploaderIT.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;
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

  // Test multiple csv creation on reaching size limit
  @Test
  public void testSerializeCSVMultiple() throws Exception {
    // after each write, the size will exceed 1 byte, so there should be 2 files
    bindUploader.setFileSize(1);

    bindUploader.upload(getBindings(conn));

    // CSV should exist in bind directory until the uploader is closed
    Path p = bindUploader.getBindDir();
    assertTrue(Files.exists(p));
    assertTrue(Files.isDirectory(p));

    File[] files = p.toFile().listFiles();
    Arrays.sort(files);
    assertNotNull(files);
    assertEquals(2, files.length);
    File csvFile = files[0];
    try (BufferedReader br =
        new BufferedReader(
            new InputStreamReader(new GZIPInputStream(new FileInputStream(csvFile))))) {
      assertEquals(csv1, br.readLine());
      assertNull(br.readLine());
    }

    csvFile = files[1];
    try (BufferedReader br =
        new BufferedReader(
            new InputStreamReader(new GZIPInputStream(new FileInputStream(csvFile))))) {
      assertEquals(csv2, br.readLine());
      assertNull(br.readLine());
    }

    bindUploader.close();

    // After the uploader closes, it should clean up the CSV
    assertFalse(Files.exists(p));
  }
  // Test multiple csv upload and successful parsing
  @Test
  public void testUploadedResultsMultiple() throws Exception {
    // after each write, the size will exceed 1 byte, so there should be 2 files
    bindUploader.setFileSize(1);

    bindUploader.upload(getBindings(conn));

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(SELECT_FROM_STAGE);
    rs.next();
    assertEquals(csv1, parseRow(rs));
    rs.next();
    assertEquals(csv2, parseRow(rs));
    assertFalse(rs.next());
  }

  // Test single csv upload and successful parsing
  @Test
  public void testUploadedResultsSimple() throws Exception {
    bindUploader.upload(getBindings(conn));

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(SELECT_FROM_STAGE);
    rs.next();
    assertEquals(csv1, parseRow(rs));
    rs.next();
    assertEquals(csv2, parseRow(rs));
    assertFalse(rs.next());
  }

  // Test csv correctness, and deletion after close
  @Test
  public void testSerializeCSVSimple() throws Exception {
    bindUploader.upload(getBindings(conn));

    // CSV should exist in bind directory until the uploader is closed
    Path p = bindUploader.getBindDir();
    assertTrue(Files.exists(p));
    assertTrue(Files.isDirectory(p));

    File[] files = p.toFile().listFiles();
    assertNotNull("file must exists", files);
    assertEquals(files.length, 1);

    File csvFile = files[0];
    try (BufferedReader br =
        new BufferedReader(
            new InputStreamReader(new GZIPInputStream(new FileInputStream(csvFile))))) {
      assertEquals(csv1, br.readLine());
      assertEquals(csv2, br.readLine());
      assertNull(br.readLine());
    }

    bindUploader.close();

    // After the uploader closes, it should clean up the CSV
    assertFalse(Files.exists(p));
  }
}
