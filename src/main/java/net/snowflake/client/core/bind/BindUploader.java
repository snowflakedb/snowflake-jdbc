/*
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.bind;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SFPair;
import net.snowflake.common.core.SqlState;

public class BindUploader implements Closeable {
  private static final SFLogger logger = SFLoggerFactory.getLogger(BindUploader.class);

  private static final String PREFIX = "binding_";

  private static final String STAGE_NAME = "SYSTEM$BIND";

  private static final String CREATE_STAGE_STMT =
      "CREATE TEMPORARY STAGE "
          + STAGE_NAME
          + " file_format=("
          + " type=csv"
          + " field_optionally_enclosed_by='\"'"
          + ")";

  private static final String PUT_STMT =
      "PUT"
          + " 'file://%s%s*'" // argument 1: folder, argument 2: separator
          + " '%s'" // argument 3: stage path
          + " parallel=10" // upload chunks in parallel
          + " overwrite=true" // skip file existence check
          + " auto_compress=false" // we compress already
          + " source_compression=gzip"; //   (with gzip)

  private static final int PUT_RETRY_COUNT = 3;

  // session of the uploader
  private final SFSession session;

  // fully-qualified stage path to upload binds to
  private final String stagePath;

  // local directory to write binds to
  private final Path bindDir;

  // whether the uploader has completed
  private boolean closed = false;

  // size (bytes) per file in upload, 100MB default
  private long fileSize = 100 * 1024 * 1024;

  // size (bytes) of max input stream
  private long inputStreamBufferSize = 8 * 1024;

  private final DateFormat timestampFormat;
  private final DateFormat dateFormat;
  private final SimpleDateFormat timeFormat;

  static class ColumnTypeDataPair {
    public String type;
    public List<String> data;

    ColumnTypeDataPair(String type, List<String> data) {
      this.type = type;
      this.data = data;
    }
  }

  /**
   * Create a new BindUploader which will write binds to the *existing* bindDir and upload them to
   * the given stageDir
   *
   * @param session the session to use for uploading binds
   * @param stageDir the stage path to upload to
   * @param bindDir the local directory to serialize binds to
   */
  private BindUploader(SFSession session, String stageDir, Path bindDir) {
    this.session = session;
    this.stagePath = "@" + STAGE_NAME + "/" + stageDir;
    this.bindDir = bindDir;
    Calendar calendarUTC = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    calendarUTC.clear();

    this.timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.");
    this.timestampFormat.setCalendar(calendarUTC);
    this.dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    this.dateFormat.setCalendar(calendarUTC);
    this.timeFormat = new SimpleDateFormat("HH:mm:ss.");
    this.timeFormat.setCalendar(calendarUTC);
  }

  private synchronized String synchronizedDateFormat(String o) {
    if (o == null) {
      return null;
    }
    return dateFormat.format(new java.sql.Date(Long.parseLong(o)));
  }

  private synchronized String synchronizedTimeFormat(String o) {
    if (o == null) {
      return null;
    }
    SFPair<Long, Integer> times = getNanosAndSecs(o, false);
    long sec = times.left;
    int nano = times.right;

    Time v1 = new Time(sec * 1000);
    String formatWithDate = timestampFormat.format(v1) + String.format("%09d", nano);
    // Take out the Date portion of the formatted string. Only time data is needed.
    return formatWithDate.substring(11);
  }

  private SFPair<Long, Integer> getNanosAndSecs(String o, boolean isNegative) {
    String inpString = o;
    if (isNegative) {
      inpString = o.substring(1);
    }

    long sec;
    int nano;
    if (inpString.length() < 10) {
      sec = 0;
      nano = Integer.parseInt(inpString);
    } else {
      sec = Long.parseLong(inpString.substring(0, inpString.length() - 9));
      nano = Integer.parseInt(inpString.substring(inpString.length() - 9));
    }
    if (isNegative) {
      // adjust the timestamp
      sec = -1 * sec;
      if (nano > 0) {
        nano = 1000000000 - nano;
        sec--;
      }
    }
    return SFPair.of(sec, nano);
  }

  private synchronized String synchronizedTimestampFormat(String o) {
    if (o == null) {
      return null;
    }

    boolean isNegative = o.length() > 0 && o.charAt(0) == '-';
    SFPair<Long, Integer> times = getNanosAndSecs(o, isNegative);
    long sec = times.left;
    int nano = times.right;

    Timestamp v1 = new Timestamp(sec * 1000);
    return timestampFormat.format(v1) + String.format("%09d", nano) + " +00:00";
  }

  /**
   * Create a new BindUploader which will upload to the given stage path Ensure temporary directory
   * for file writing exists
   *
   * @param session the session to use for uploading binds
   * @param stageDir the stage path to upload to
   * @return BindUploader instance
   * @throws BindException if temporary directory could not be created
   */
  public static synchronized BindUploader newInstance(SFSession session, String stageDir)
      throws BindException {
    try {
      Path bindDir = Files.createTempDirectory(PREFIX);
      return new BindUploader(session, stageDir, bindDir);
    } catch (IOException ex) {
      throw new BindException(
          String.format("Failed to create temporary directory: %s", ex.getMessage()),
          BindException.Type.OTHER);
    }
  }

  /**
   * Upload the bindValues to stage
   *
   * @param bindValues the bind map to upload
   * @throws BindException if the bind map could not be serialized or upload fails
   */
  public void upload(Map<String, ParameterBindingDTO> bindValues)
      throws BindException, SQLException {
    if (!closed) {
      List<ColumnTypeDataPair> columns = getColumnValues(bindValues);
      List<byte[]> csvRows = buildRowsAsBytes(columns);
      int startIndex = 0;
      int numBytes = 0;
      int rowNum = 0;
      int fileCount = 0;

      while (rowNum < csvRows.size()) {
        // create a list of byte arrays
        while (numBytes < inputStreamBufferSize && rowNum < csvRows.size()) {
          numBytes += csvRows.get(rowNum).length;
          rowNum++;
        }
        // concatenate all byte arrays into 1 and put into input stream
        ByteBuffer bb = ByteBuffer.allocate(numBytes);
        for (int i = startIndex; i < rowNum; i++) {
          bb.put(csvRows.get(i));
        }
        byte[] finalBytearray = bb.array();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(finalBytearray);
        // do the upload
        String fileName = Integer.toString(++fileCount);
        uploadStreamInternal(inputStream, fileName, true);
        // reset for next round. StartIndex leaves off from byte array. NumBytes = 0.
        startIndex = rowNum;
        numBytes = 0;
        try {
          inputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Method to put data from a stream at a stage location. The data will be uploaded as one file. No
   * splitting is done in this method.
   *
   * <p>Stream size must match the total size of data in the input stream unless compressData
   * parameter is set to true.
   *
   * <p>caller is responsible for passing the correct size for the data in the stream and releasing
   * the inputStream after the method is called.
   *
   * @param inputStream input stream from which the data will be uploaded
   * @param destFileName destination file name to use
   * @param compressData whether compression is requested fore uploading data
   * @throws SQLException raises if any error occurs
   */
  private void uploadStreamInternal(
      InputStream inputStream, String destFileName, boolean compressData)
      throws SQLException, BindException {

    createStageIfNeeded();
    String stageName = stagePath;
    logger.debug("upload data from stream: stageName={}" + ", destFileName={}", destFileName);

    if (stageName == null) {
      throw new SnowflakeSQLLoggedException(
          session,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "stage name is null");
    }

    if (destFileName == null) {
      throw new SnowflakeSQLLoggedException(
          session,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "stage name is null");
    }

    SFStatement stmt = new SFStatement(session);

    StringBuilder putCommand = new StringBuilder();

    // use a placeholder for source file
    putCommand.append("put file:///tmp/placeholder ");

    // add stage name
    putCommand.append("'");
    if (!(stageName.startsWith("@") || stageName.startsWith("'@") || stageName.startsWith("$$@"))) {
      putCommand.append("@");
    }
    putCommand.append(stageName);
    putCommand.append("'");

    putCommand.append(" overwrite=true");

    SnowflakeFileTransferAgent transferAgent;
    transferAgent = new SnowflakeFileTransferAgent(putCommand.toString(), session, stmt);

    transferAgent.setSourceStream(inputStream);
    transferAgent.setDestFileNameForStreamSource(destFileName);
    transferAgent.setCompressSourceFromStream(compressData);
    transferAgent.execute();

    stmt.close();
  }

  /**
   * Convert bind map to a list of values for each column Perform necessary type casts and invariant
   * checks
   *
   * @param bindValues the bind map to convert
   * @return list of values for each column
   * @throws BindException if bind map is improperly formed
   */
  private List<ColumnTypeDataPair> getColumnValues(Map<String, ParameterBindingDTO> bindValues)
      throws BindException {
    List<ColumnTypeDataPair> columns = new ArrayList<>(bindValues.size());
    for (int i = 1; i <= bindValues.size(); i++) {
      // bindValues should have n entries with string keys 1 ... n and list values
      String key = Integer.toString(i);
      if (!bindValues.containsKey(key)) {
        throw new BindException(
            String.format(
                "Bind map with %d columns should contain key \"%d\"", bindValues.size(), i),
            BindException.Type.SERIALIZATION);
      }

      ParameterBindingDTO value = bindValues.get(key);
      try {
        String type = value.getType();
        List<?> list = (List<?>) value.getValue();
        List<String> convertedList = new ArrayList<>(list.size());
        if ("TIMESTAMP_LTZ".equals(type) || "TIMESTAMP_NTZ".equals(type)) {
          for (Object e : list) {
            convertedList.add(synchronizedTimestampFormat((String) e));
          }
        } else if ("DATE".equals(type)) {
          for (Object e : list) {
            convertedList.add(synchronizedDateFormat((String) e));
          }
        } else if ("TIME".equals(type)) {
          for (Object e : list) {
            convertedList.add(synchronizedTimeFormat((String) e));
          }
        } else {
          for (Object e : list) {
            convertedList.add((String) e);
          }
        }
        columns.add(i - 1, new ColumnTypeDataPair(type, convertedList));
      } catch (ClassCastException ex) {
        throw new BindException(
            "Value in binding DTO could not be cast to a list", BindException.Type.SERIALIZATION);
      }
    }
    return columns;
  }

  /**
   * Transpose a list of columns and their values to a list of rows in bytes
   *
   * @param columns the list of columns to transpose
   * @return list of rows
   * @throws BindException if columns improperly formed
   */
  private List<byte[]> buildRowsAsBytes(List<ColumnTypeDataPair> columns) throws BindException {
    List<byte[]> rows = new ArrayList<>();
    int numColumns = columns.size();
    // columns should have binds
    if (columns.get(0).data.isEmpty()) {
      throw new BindException("No binds found in first column", BindException.Type.SERIALIZATION);
    }

    int numRows = columns.get(0).data.size();
    // every column should have the same number of binds
    for (int i = 0; i < numColumns; i++) {
      int iNumRows = columns.get(i).data.size();
      if (columns.get(i).data.size() != numRows) {
        throw new BindException(
            String.format(
                "Column %d has a different number of binds (%d) than column 1 (%d)",
                i, iNumRows, numRows),
            BindException.Type.SERIALIZATION);
      }
    }

    for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
      String[] row = new String[numColumns];
      for (int colIdx = 0; colIdx < numColumns; colIdx++) {
        row[colIdx] = columns.get(colIdx).data.get(rowIdx);
      }
      rows.add(createCSVRecord(row));
    }

    return rows;
  }

  /**
   * Serialize row to a csv Duplicated from StreamLoader class
   *
   * @param data the row to create a csv record from
   * @return serialized csv for row
   */
  private byte[] createCSVRecord(String[] data) {
    StringBuilder sb = new StringBuilder(1024);

    for (int i = 0; i < data.length; ++i) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(SnowflakeType.escapeForCSV(data[i]));
    }
    sb.append('\n');
    return sb.toString().getBytes(UTF_8);
  }

  /**
   * Check whether the session's temporary stage has been created, and create it if not.
   *
   * @throws BindException if creating the stage fails
   */
  private void createStageIfNeeded() throws BindException {
    if (session.getArrayBindStage() != null) {
      return;
    }
    synchronized (session) {
      // another thread may have created the session by the time we enter this block
      if (session.getArrayBindStage() == null) {
        try {
          SFStatement statement = new SFStatement(session);
          statement.execute(CREATE_STAGE_STMT, false, null, null);
          session.setArrayBindStage(STAGE_NAME);
        } catch (SFException | SQLException ex) {
          // to avoid repeated failures to create stage, disable array bind stage
          // optimization if we fail to create stage for some reason
          session.setArrayBindStageThreshold(0);
          throw new BindException(
              String.format(
                  "Failed to create temporary stage for array binds. %s", ex.getMessage()),
              BindException.Type.UPLOAD);
        }
      }
    }
  }

  /**
   * Close uploader, deleting the local temporary directory
   *
   * <p>This class can be used in a try-with-resources statement, which ensures that the temporary
   * directory is cleaned up even when exceptions occur
   */
  @Override
  public void close() {
    if (!closed) {
      try {
        if (Files.isDirectory(bindDir)) {
          String[] dir = bindDir.toFile().list();
          if (dir != null) {
            for (String fileName : dir) {
              Files.delete(bindDir.resolve(fileName));
            }
          }
          Files.delete(bindDir);
        }
      } catch (IOException ex) {
        logger.debug("Exception encountered while trying to clean local directory. ", ex);
      } finally {
        closed = true;
      }
    }
  }

  /**
   * Set the approximate maximum size in bytes for a single bind file
   *
   * @param fileSize size in bytes
   */
  public void setFileSize(int fileSize) {
    this.fileSize = fileSize;
  }

  /**
   * Return the stage path to which binds are uploaded
   *
   * @return the stage path
   */
  public String getStagePath() {
    return this.stagePath;
  }

  /**
   * Return the local path to which binds are serialized
   *
   * @return the local path
   */
  public Path getBindDir() {
    return this.bindDir;
  }

  /**
   * Compute the number of array bind values in the given bind map
   *
   * @param bindValues the bind map
   * @return 0 if bindValues is null, has no binds, or is not an array bind n otherwise, where n is
   *     the number of binds in the array bind
   */
  public static int arrayBindValueCount(Map<String, ParameterBindingDTO> bindValues) {
    if (!isArrayBind(bindValues)) {
      return 0;
    } else {
      ParameterBindingDTO bindSample = bindValues.values().iterator().next();
      List<?> bindSampleValues = (List<?>) bindSample.getValue();
      return bindValues.size() * bindSampleValues.size();
    }
  }

  /**
   * Return whether the bind map uses array binds
   *
   * @param bindValues the bind map
   * @return whether the bind map uses array binds
   */
  public static boolean isArrayBind(Map<String, ParameterBindingDTO> bindValues) {
    if (bindValues == null || bindValues.size() == 0) {
      return false;
    }
    ParameterBindingDTO bindSample = bindValues.values().iterator().next();
    return bindSample.getValue() instanceof List;
  }
}
