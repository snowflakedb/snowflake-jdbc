/*
 * Copyright (c) 2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import static net.snowflake.client.core.SFException.errorResourceBundleManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

public class IncidentIT extends BaseIncidentTest {
  // Copied from StringLimiter in GS
  private static int countLines(String input) {
    if (input == null || input.isEmpty()) {
      return 0;
    }

    int lines = 1;
    Matcher m = Pattern.compile("(\r\n)|(\r)|(\n)").matcher(input);
    while (m.find()) {
      lines++;
    }
    return lines;
  }

  /** Create an incident from pieces, explicit signature */
  @Test
  public void SimpleIncidentCreationTestExplicit() {
    // Constants
    String jobId = "ji";
    String requestId = "ri";
    String raiser =
        "net.snowflake.client.core.IncidentIT$"
            + "CreateIncidentTests.SimpleIncidentCreationTestExpliciit";
    String errorMessage = "error Message";
    String errorStackTrace = "this is a stack element\nthis is another " + "element";
    Incident incident =
        new Incident(new SFSession(), jobId, requestId, errorMessage, errorStackTrace, raiser);
    Assert.assertEquals(jobId, incident.jobId);
    Assert.assertEquals(requestId, incident.requestId);
    Assert.assertTrue(incident.signature.startsWith(errorMessage + " at " + raiser));
    Assert.assertEquals(errorMessage, incident.errorMessage);
    Assert.assertEquals(errorStackTrace, incident.errorStackTrace);
    Assert.assertNotNull(incident.osName);
    Assert.assertNotNull(incident.osVersion);
    Assert.assertNotNull(incident.timestamp);
    Assert.assertEquals(Event.EventType.INCIDENT, incident.getType());
  }

  @Test
  public void testEventFunctions() {
    // Constants
    String jobId = "ji";
    String requestId = "ri";
    String raiser =
        "net.snowflake.client.core.IncidentIT$"
            + "CreateIncidentTests.SimpleIncidentCreationTestExpliciit";
    String errorMessage = "error Message";
    String errorStackTrace = "this is a stack element\nthis is another " + "element";
    Incident incident =
        new Incident(new SFSession(), jobId, requestId, errorMessage, errorStackTrace, raiser);
    incident.setType(Event.EventType.NETWORK_ERROR);
    Assert.assertEquals(Event.EventType.NETWORK_ERROR, incident.getType());
    String newMessage = "hello";
    incident.setMessage(newMessage);
    Assert.assertEquals(newMessage, incident.getMessage());
    incident.writeEventDumpLine("sample dump line");
  }

  /** Create an incident from a dummy RuntimeException */
  @Test
  @SuppressWarnings("ThrowableNotThrown")
  public void SimpleIncidentCreationTestRuntimeException() {
    // Constants
    String jobId = "ji";
    String requestId = "ri";
    String errorMessage = "This is a test exception";

    ErrorCode ec = ErrorCode.INTERNAL_ERROR;
    SFException exc = new SFException(ec, errorMessage);
    Incident incident = new Incident(new SFSession(), exc, jobId, requestId);
    Assert.assertEquals("jdbc", incident.driverName);
    Assert.assertNotNull(incident.driverVersion);
    Assert.assertNotNull(incident.signature);
    Assert.assertEquals(
        errorResourceBundleManager.getLocalizedMessage(
            String.valueOf(ec.getMessageCode()), errorMessage),
        incident.errorMessage);
    Assert.assertTrue(countLines(incident.errorStackTrace) > 1);
    Assert.assertNotNull(incident.osName);
    Assert.assertNotNull(incident.osVersion);
    Assert.assertNotNull(incident.timestamp);
    Assert.assertEquals(jobId, incident.jobId);
    Assert.assertEquals(requestId, incident.requestId);
    Assert.assertEquals(Event.EventType.INCIDENT, incident.getType());
  }

  /** Create an incident from a dummy RuntimeException */
  @Test
  @SuppressWarnings("ThrowableNotThrown")
  public void VerifyUniqueUUIDTest() {
    // Constants
    String jobId = "ji";
    String requestId = "ri";
    String errorMessage = "This is a test exception";

    SFException exc = new SFException(ErrorCode.INTERNAL_ERROR, errorMessage);
    Incident incident1 = new Incident(new SFSession(), exc, jobId, requestId);
    Incident incident2 = new Incident(new SFSession(), exc, jobId, requestId);
    Assert.assertNotEquals(incident1.uuid, incident2.uuid);
  }

  /** Tests for triggering a client incident on GS */
  @Test
  public void simpleFlushTest() throws SQLException {
    String jobId = "ji";
    String requestId = "ri";
    String errorMessage = RandomStringUtils.randomAlphabetic(5);
    SFException exc = new SFException(ErrorCode.INTERNAL_ERROR, errorMessage);
    String expected_signature =
        errorMessage
            + ". at net.snowflake.client.core"
            + ".IncidentIT.simpleFlushTest(IncidentIT.java:";
    Connection connection = getConnection();

    Incident incident =
        new Incident(
            connection.unwrap(SnowflakeConnectionV1.class).getSfSession(), exc, jobId, requestId);
    incident.trigger();
    verifyIncidentRegisteredInGS(expected_signature, 1);
    connection.close();
  }

  /** See if dump file is created by client */
  @Test
  public void testDumpFileCreated() throws Throwable {
    String errorMessage = "testDumpFile" + RandomStringUtils.randomAlphabetic(5);
    SFException exc = new SFException(ErrorCode.INTERNAL_ERROR, errorMessage);
    Connection connection = getConnection();
    Incident incident =
        new Incident(
            connection.unwrap(SnowflakeConnectionV1.class).getSfSession(), exc, "ji", "ri");
    incident.trigger();
    String dumpFile = findDmpFile(incident.signature);
    File file = new File(dumpFile);
    Assert.assertTrue(file.isFile());

    Scanner scanner = new Scanner(file);
    while (scanner.hasNextLine()) {
      String line = scanner.nextLine();
      if (line.contains(incident.signature)) {
        scanner.close();
        cleanUpDmpFile(dumpFile);
        return;
      }
    }
    Assert.fail("must find the signature");
  }

  /** Helper function to get dump file for an incident from GS */
  private String findDmpFile(String signature) throws Throwable {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    String dpoScanString =
        "select $1:\"WAIncidentDPO:primary\":\"summary\""
            + "from table(dposcan('{\"slices\" : [{\"name\" : \"WAIncidentDPO:primary\"}]}')) dpo "
            + "where $1:\"WAIncidentDPO:primary\":\"sourceErrorSignature\" = '"
            + signature
            + "'";

    statement.execute(dpoScanString);
    ResultSet resultSet = statement.getResultSet();
    Assert.assertTrue(resultSet.next());
    String summary = resultSet.getString(1);
    connection.close();

    ObjectMapper mapper = new ObjectMapper();
    summary = (summary.substring(1, summary.length() - 1)).replace("\\", "");
    JsonNode result = mapper.readValue(summary, JsonNode.class);
    return result.path("dumpFile").asText();
  }

  /** Helper to clean up dump file */
  private void cleanUpDmpFile(String dumpFile) {
    // check if the file name format is what we expected
    String regex = ".*/logs/gs_.*dmp";
    Assert.assertTrue(dumpFile.matches(regex));
    File file = new File(dumpFile);
    Assert.assertTrue(file.isFile());
    Assert.assertTrue(file.delete());
  }

  /** */
  @Test
  public void fullTriggerIncident() throws SQLException {
    SFException exc =
        new SFException(
            ErrorCode.IO_ERROR,
            "Mark Screwed something up again" + RandomStringUtils.randomAlphabetic(3));
    Connection connection = getConnection();
    SFSession session = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
    Incident incident = new Incident(session, exc, null, null);
    String signature = incident.signature;
    try {
      // This is how incidents should be raised from now on
      throw (SFException) IncidentUtil.generateIncidentV2WithException(session, exc, null, null);
    } catch (SFException ex) {
      verifyIncidentRegisteredInGS(signature, 1);
    } finally {
      connection.close();
    }
  }
}
