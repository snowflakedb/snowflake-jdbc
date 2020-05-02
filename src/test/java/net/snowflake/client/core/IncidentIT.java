/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnTravisCI;
import net.snowflake.client.category.TestCategoryCore;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test Incident framework
 */
@Category(TestCategoryCore.class)
public class IncidentIT extends BaseIncidentTest
{
  /**
   * Helper function to delete an entire directory
   */
  private static boolean deleteDirectory(File directory)
  {
    if (directory.exists())
    {
      if (directory.listFiles() != null)
      {
        File[] files = directory.listFiles();

        for (File file : files)
        {
          if (file.isDirectory())
          {
            deleteDirectory(file);
          }
          else
          {
            file.delete();
          }
        }
      }
    }

    return directory.delete();
  }

  /*
   * Test creation of a simple incident
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testFlushEvent() throws Throwable
  {
    EventHandler eventHandler = EventUtil.getEventHandlerInstance();
    eventHandler.stopFlusher();

    eventHandler.triggerBasicEvent(Event.EventType.NONE, "Flush buffer", true);

    // Does event buffer get flushed at capacity?
    for (int i = 0; i < 1000; i++)
    {
      eventHandler.triggerBasicEvent(Event.EventType.NONE, "Event " + i);
    }

    assertEquals(0, eventHandler.getBufferSize());

    // Does event buffer get flushed by thread?
    eventHandler.startFlusher();
    eventHandler.triggerBasicEvent(Event.EventType.NONE, "Event 6");

    // Wait for max 1 minute
    int retry = 0;
    while (retry < 120 && eventHandler.getBufferSize() > 0)
    {
      Thread.sleep(500);
      retry++;
    }

    assertEquals(String.format("eventHandler.size=%d", eventHandler.getBufferSize()),
                 0, eventHandler.getBufferSize());
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testTriggerIncident() throws SQLException
  {
    Connection connection;
    Statement statement = null;

    connection = getConnection();
    connection.createStatement().execute("alter session set SUPPRESS_INCIDENT_DUMPS=true");

    // Should now trigger an incident
    try
    {
      statement = connection.createStatement();
      System.setProperty("snowflake.enable_incident_test1", Boolean.TRUE.toString());
      statement.execute("create or replace temp table jdbc_tmp(C1 INT)");
      fail("create temp must fail");
    }
    catch (Throwable ex)
    {
      System.clearProperty("snowflake.enable_incident_test1");
      verifyIncidentRegisteredInGS("Statement is closed.", -1);
    }
    finally
    {
      if (statement != null)
      {
        statement.close();
      }
    }

    try
    {
      statement = connection.createStatement();
      System.setProperty("snowflake.enable_incident_test2", Boolean.TRUE.toString());

      statement.execute("create or replace temp table jdbc_tmp(C1 INT)");
      ResultSet r = statement.executeQuery("select * from jdbc_tmp");
      r.next();
      fail("select must fail");
    }
    catch (Throwable ex)
    {
      System.clearProperty("snowflake.enable_incident_test2");
      verifyIncidentRegisteredInGS("Maximum size for a query result has been exceeded.", -1);
    }
    finally
    {
      statement.close();
    }

    connection.close();
  }

  /**
   * Test for the following:
   * 1) Basic creation of log dump
   * 2) Creation of log dump AND intermediate directories
   * 3) Cleanup of a single large dump file
   * 4) Limit on number of allowed dump files at any given time
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testCreateLogDump()
  {
    // Test dump file is created
    EventHandler eh = EventUtil.getEventHandlerInstance();
    eh.publish(new LogRecord(Level.WARNING, RandomStringUtils.random(64)));
    eh.dumpLogBuffer("test");

    // Does directory exist?
    File dumpdir = new File(EventUtil.getDumpPathPrefix());
    assertTrue(dumpdir.exists() && dumpdir.isDirectory());

    // Does file exist?
    File dumpfile = new File(EventUtil.getDumpPathPrefix() + "/" +
                             EventHandler.LOG_DUMP_FILE_NAME + "test" +
                             EventHandler.LOG_DUMP_FILE_EXT);
    assertTrue(dumpfile.exists());

    // Next, check that if the dump directory does not exist, it can be created
    deleteDirectory(dumpdir);
    dumpdir = new File(EventUtil.getDumpPathPrefix());
    assertTrue(!dumpdir.exists());

    eh.publish(new LogRecord(Level.WARNING, RandomStringUtils.random(64)));
    eh.dumpLogBuffer("test");

    dumpdir = new File(EventUtil.getDumpPathPrefix());
    assertTrue(dumpdir.exists());

    // Now test the creation and cleanup a single large dump file
    for (long i = 0; i < EventHandler.LOG_BUFFER_SIZE; i++)
      eh.publish(new LogRecord(Level.WARNING, RandomStringUtils.random(64)));

    eh.dumpLogBuffer("test");
    dumpfile = new File(EventUtil.getDumpPathPrefix() + "/" +
                        EventHandler.LOG_DUMP_FILE_NAME + "test" +
                        EventHandler.LOG_DUMP_FILE_EXT);

    assertTrue(dumpfile.exists() && dumpfile.length() >= (maxDumpSizeMb << 20));

    eh.cleanupSfDumps(false);
    dumpfile = new File(EventUtil.getDumpPathPrefix() + "/" +
                        EventHandler.LOG_DUMP_FILE_NAME + "test" +
                        EventHandler.LOG_DUMP_FILE_EXT);
    assertTrue(!dumpfile.exists());

    for (int i = 0; i < maxLogDumps + 3; i++)
    {
      eh.publish(new LogRecord(Level.WARNING, RandomStringUtils.random(64)));
      eh.dumpLogBuffer("test" + i);
    }

    dumpdir = new File(EventUtil.getDumpPathPrefix());
    File[] files = dumpdir.listFiles();
    assertNotNull(files);
    assertEquals(maxLogDumps, files.length);

    deleteDirectory(dumpdir);
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testDumpFileCreated() throws Throwable
  {
    String signature = "testDumpFile" + RandomStringUtils.randomAlphabetic(5);
    String expectedSignaturePrefix = "Non-Fatal incident: " + signature + ". ";
    generateIncidentWithSignature(signature, false);
    String dumpFile = findDmpFile(expectedSignaturePrefix);
    File file = new File(dumpFile);
    assertTrue(file.isFile());

    Scanner scanner = new Scanner(file);
    while (scanner.hasNextLine())
    {
      String line = scanner.nextLine();
      // TODO - parse json to check if signature matches
      if (line.contains(signature))
      {
        scanner.close();
        cleanUpDmpFile(dumpFile);
        return;
      }
    }
    fail("must find the signature");
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testGenerateIncidentsInMultipleThreads() throws Throwable
  {
    final String signature = "testGenerateIncidentsInMultipleThreads"
                             + RandomStringUtils.randomAlphabetic(5);
    class generateIncidentThread implements Runnable
    {
      @Override
      public void run()
      {
        try
        {
          generateIncidentWithSignature(
              signature + RandomStringUtils.randomAlphabetic(7),
              true);
        }
        catch (SQLException e)
        {
          fail("must success");
        }
      }
    }

    List<Thread> pool = new ArrayList<>();
    for (int i = 0; i < 10; i++)
    {
      pool.add(new Thread(new generateIncidentThread()));
      pool.get(i).start();
    }
    for (Thread t : pool)
    {
      t.join();
    }

    verifyIncidentRegisteredInGS(signature, 10);

  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testThrottleIncidentsInClientSide() throws SQLException
  {
    String signature = "testThrottleIncidentsInClientSide"
                       + RandomStringUtils.randomAlphabetic(5);
    generateIncidentWithSignature(signature + "1", true);
    verifyIncidentRegisteredInGS(signature + "1", 1);

    generateIncidentWithSignature(signature + "2", true);
    verifyIncidentRegisteredInGS(signature + "2", 1);
  }


  @Ignore
  @Test
  public void testThrottleIncidentsInServerSide() throws Throwable
  {
    String signatureHeader = "testThrottleIncidentsInServerSide";
    Connection snowflake = getConnection();
    Statement statement = snowflake.createStatement();
    ResultSet rs = statement.executeQuery("alter system set CLIENT_INCIDENTS_THROTTLE_WINDOW_MINS=1");
    rs.next();
    assertEquals("Statement executed successfully.", rs.getString(1));

    rs = statement.executeQuery("alter system set CLIENT_INCIDENT_ACCT_THROTTLE_LIM=3");
    rs.next();
    assertEquals("Statement executed successfully.", rs.getString(1));
    try
    {
      // wait 1 min to unthrottle incidents
      Thread.sleep(70000);

      // used to differentiate from multiple runs
      String testTag = RandomStringUtils.randomAlphabetic(6);
      for (int i = 0; i < 3; i++)
      {
        generateIncidentWithSignature(signatureHeader + testTag + RandomStringUtils.randomAlphabetic(3), true);
      }
      verifyIncidentRegisteredInGS(signatureHeader + testTag, 2);
      // hit limit should not register in GS
      generateIncidentWithSignature(signatureHeader + testTag + "11", true);
      verifyIncidentRegisteredInGS(signatureHeader + testTag, 2);

      Thread.sleep(70000);
      generateIncidentWithSignature(signatureHeader + testTag + "12", true);
      verifyIncidentRegisteredInGS(signatureHeader + testTag, 3);
    }
    finally
    {
      Statement st = snowflake.createStatement();
      st.execute("alter system set " +
                 "CLIENT_INCIDENTS_THROTTLE_WINDOW_MINS=default");
      st.execute("alter system set CLIENT_INCIDENT_ACCT_THROTTLE_LIM=1000");
      snowflake.close();
    }

  }

  private String findDmpFile(String signature) throws Throwable
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    String dpoScanString = "select $1:\"WAIncidentDPO:primary\":\"summary\"" +
                           "from table(dposcan('{\"slices\" : [{\"name\" : \"WAIncidentDPO:primary\"}]}')) dpo " +
                           "where $1:\"WAIncidentDPO:primary\":\"sourceErrorSignature\" " +
                           "like '" + signature + "%'";

    statement.execute(dpoScanString);
    ResultSet resultSet = statement.getResultSet();
    assertTrue(resultSet.next());
    String summary = resultSet.getString(1);
    connection.close();

    ObjectMapper mapper = new ObjectMapper();
    summary = (summary.substring(1, summary.length() - 1)).replace("\\", "");
    JsonNode result = mapper.readValue(summary, JsonNode.class);
    return result.path("dumpFile").asText();
  }

  private void cleanUpDmpFile(String dumpFile)
  {
    // check if the file name format is what we expected
    String regex = ".*/logs/gs_.*dmp";
    assertTrue(dumpFile.matches(regex));
    File file = new File(dumpFile);
    assertTrue(file.isFile());
    assertTrue(file.delete());
  }
}
