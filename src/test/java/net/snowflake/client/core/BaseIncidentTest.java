/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class BaseIncidentTest extends AbstractDriverIT {
  static int maxLogDumps = 5;
  static long maxDumpSizeMb = 1;

  @BeforeClass
  public static void setUpClass() throws SQLException {
    Connection connection = getSnowflakeAdminConnection();
    Statement statement = connection.createStatement();
    statement.execute(
        "alter system set "
            + " CLIENT_DISABLE_INCIDENTS=false"
            + ",ENABLE_CLIENT_SIDE_INCIDENTS=true"
            + ",CLIENT_INCIDENT_ACCT_THROTTLE_LIM=1000");
    statement.close();
    connection.close();
  }

  @AfterClass
  public static void tearDownClass() throws SQLException {
    Connection connection = getSnowflakeAdminConnection();
    Statement statement = connection.createStatement();
    statement.execute(
        "alter system set "
            + " CLIENT_DISABLE_INCIDENTS=default"
            + ",ENABLE_CLIENT_SIDE_INCIDENTS=default"
            + ",CLIENT_INCIDENT_ACCT_THROTTLE_LIM=default");
    statement.close();
    connection.close();
  }

  @Before
  public void setUp() {
    System.setProperty(EventHandler.MAX_SIZE_DUMPS_MB_PROP, String.valueOf(maxDumpSizeMb));
    System.setProperty(EventHandler.MAX_NUM_DUMP_FILES_PROP, String.valueOf(maxLogDumps));
    System.setProperty(EventHandler.DISABLE_DUMP_COMPR_PROP, Boolean.TRUE.toString());
  }

  @After
  public void tearDown() {}

  @SuppressWarnings("ThrowableNotThrown")
  void generateIncidentWithSignature(String signature, boolean silenceIncidents)
      throws SQLException {
    Connection connection = getSnowflakeAdminConnection();
    if (silenceIncidents) {
      connection.createStatement().execute("alter session set SUPPRESS_INCIDENT_DUMPS=true");
    }
    SFSession session = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
    IncidentUtil.generateIncidentV2WithException(
        session, new SFException(ErrorCode.NON_FATAL_ERROR, signature), null, null);
    connection.close();
  }

  // If the expectedNumberOfIncidents is -1, it indicates we don't
  // care about exact number of records
  void verifyIncidentRegisteredInGS(String sourceErrorSignature, int expectedNumberOfIncidents)
      throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute(
        "select $1:\"WAIncidentDPO:primary\" "
            + "from table(dposcan('"
            + "{\"slices\" : [{\"name\" : \"WAIncidentDPO:primary\"}]}')) dpo "
            + "where $1:\"WAIncidentDPO:primary\":\"sourceErrorSignature\" like '%"
            + sourceErrorSignature
            + "%'");
    ResultSet resultSet = statement.getResultSet();
    int actualCount = 0;
    while (resultSet.next()) {
      actualCount++;
    }

    if (expectedNumberOfIncidents == -1) {
      assertTrue("actualCount is not positive: " + actualCount, actualCount > 0);
    } else {
      assertEquals(expectedNumberOfIncidents, actualCount);
    }

    connection.close();
  }
}
