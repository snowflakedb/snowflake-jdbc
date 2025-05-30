package net.snowflake.client.core.bind;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFBaseStatement;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SFBaseFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SFPair;
import net.snowflake.common.core.SqlState;

public class BindUploader implements Closeable {
  private static final SFLogger logger = SFLoggerFactory.getLogger(BindUploader.class);

  // session of the uploader
  private final SFBaseSession session;

  // fully-qualified stage path to upload binds to
  private final String stagePath;

  // whether the uploader has completed
  private boolean closed = false;

  // size (bytes) of max input stream (10MB default)
  private long inputStreamBufferSize = 1024 * 1024 * 10;

  private int fileCount = 0;

  private final DateTimeFormatter timestampFormatter =
      new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS "))
          .appendOffset("+HH:MM", "Z")
          .toFormatter();
  private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  private final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS");

  private final String createStageSQL;

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
   */
  private BindUploader(SFBaseSession session, String stageDir) {
    this.session = session;
    this.stagePath = "@" + session.getSfConnectionHandler().getBindStageName() + "/" + stageDir;
    this.createStageSQL =
        "CREATE TEMPORARY STAGE IF NOT EXISTS "
            + session.getSfConnectionHandler().getBindStageName()
            + " file_format=("
            + " type=csv"
            + " field_optionally_enclosed_by='\"'"
            + ")";
  }

  private synchronized String synchronizedDateFormat(String o) {
    if (o == null) {
      return null;
    }
    long millis = Long.parseLong(o);
    Instant instant = Instant.ofEpochMilli(millis);
    LocalDate localDate = instant.atZone(ZoneOffset.UTC).toLocalDate();
    return localDate.format(this.dateFormatter);
  }

  private synchronized String synchronizedTimeFormat(String o) {
    if (o == null) {
      return null;
    }
    SFPair<Long, Integer> times = getNanosAndSecs(o, false);
    long sec = times.left;
    int nano = times.right;

    LocalTime time = Instant.ofEpochSecond(sec, nano).atZone(ZoneOffset.UTC).toLocalTime();
    return time.format(timeFormatter);
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

  private synchronized String synchronizedTimestampFormat(String o, String type) {
    if (o == null) {
      return null;
    }

    boolean isNegative = o.length() > 0 && o.charAt(0) == '-';
    SFPair<Long, Integer> times = getNanosAndSecs(o, isNegative);
    long sec = times.left;
    int nano = times.right;

    Instant instant = Instant.ofEpochSecond(sec, nano);

    // For timestamp_ntz, use UTC timezone. For timestamp_ltz, use the local timezone to minimise
    // the gap.
    if ("TIMESTAMP_LTZ".equals(type)) {
      ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());
      return zdt.format(timestampFormatter);
    } else {
      ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
      return zdt.format(timestampFormatter);
    }
  }

  /**
   * Create a new BindUploader which will upload to the given stage path. Note that no temporary
   * file or directory is created anymore. Instead, streaming uploading is used.
   *
   * @param session the session to use for uploading binds
   * @param stageDir the stage path to upload to
   * @return BindUploader instance
   */
  public static synchronized BindUploader newInstance(SFBaseSession session, String stageDir) {
    return new BindUploader(session, stageDir);
  }

  /**
   * Wrapper around upload() with default compression to true.
   *
   * @param bindValues the bind map to upload
   * @throws BindException if there is an error when uploading bind values
   * @throws SQLException if any error occurs
   */
  public void upload(Map<String, ParameterBindingDTO> bindValues)
      throws BindException, SQLException {
    upload(bindValues, true);
  }

  /**
   * Upload bind parameters via streaming. This replaces previous function upload function where
   * binds were written to a file which was then uploaded with a PUT statement.
   *
   * @param bindValues the bind map to upload
   * @param compressData whether or not to compress data
   * @throws BindException if there is an error when uploading bind values
   * @throws SQLException if any error occurs
   */
  public void upload(Map<String, ParameterBindingDTO> bindValues, boolean compressData)
      throws BindException, SQLException {
    if (!closed) {
      List<ColumnTypeDataPair> columns = getColumnValues(bindValues);
      List<byte[]> bindingRows = buildRowsAsBytes(columns);
      int startIndex = 0;
      int numBytes = 0;
      int rowNum = 0;
      fileCount = 0;

      while (rowNum < bindingRows.size()) {
        // create a list of byte arrays
        while (numBytes < inputStreamBufferSize && rowNum < bindingRows.size()) {
          numBytes += bindingRows.get(rowNum).length;
          rowNum++;
        }
        // concatenate all byte arrays into 1 and put into input stream
        ByteBuffer bb = ByteBuffer.allocate(numBytes);
        for (int i = startIndex; i < rowNum; i++) {
          bb.put(bindingRows.get(i));
        }
        byte[] finalBytearray = bb.array();
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(finalBytearray)) {
          // do the upload
          String fileName = Integer.toString(++fileCount);
          uploadStreamInternal(inputStream, fileName, compressData);
          startIndex = rowNum;
          numBytes = 0;
        } catch (IOException ex) {
          throw new BindException(
              String.format(
                  "Failure using inputstream to upload bind data. Message: %s", ex.getMessage()),
              BindException.Type.SERIALIZATION);
        }
      }
    }
  }

  /**
   * Method to put data from a stream at a stage location. The data will be uploaded as one file. No
   * splitting is done in this method. Similar to uploadStreamInternal() in SnowflakeConnectionV1.
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
   * @throws BindException if there is an error when uploading bind values
   */
  private void uploadStreamInternal(
      InputStream inputStream, String destFileName, boolean compressData)
      throws SQLException, BindException {

    createStageIfNeeded();
    String stageName = stagePath;
    logger.debug(
        "upload data from stream: stageName={}" + ", destFileName={}", stageName, destFileName);

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

    SFBaseStatement stmt = session.getSfConnectionHandler().getSFStatement();

    StringBuilder putCommand = new StringBuilder();

    // use a placeholder for source file
    putCommand.append("put file:///tmp/placeholder ");

    // add stage name surrounded by quotations in case special chars are used in directory name
    putCommand.append("'");
    putCommand.append(stageName);
    putCommand.append("'");

    putCommand.append(" overwrite=true");

    SFBaseFileTransferAgent transferAgent =
        session.getSfConnectionHandler().getFileTransferAgent(putCommand.toString(), stmt);

    transferAgent.setDestStagePath(stagePath);
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
            convertedList.add(synchronizedTimestampFormat((String) e, type));
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
   * Transpose a list of columns and their values to a list of rows in bytes instead of strings
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
          SFBaseStatement statement = session.getSfConnectionHandler().getSFStatement();
          statement.execute(createStageSQL, null, null, new ExecTimeTelemetryData());
          session.setArrayBindStage(session.getSfConnectionHandler().getBindStageName());
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
      closed = true;
    }
  }

  /**
   * Set the approximate maximum size in bytes for a single bind file
   *
   * @param bufferSize size in bytes
   */
  public void setInputStreamBufferSize(int bufferSize) {
    this.inputStreamBufferSize = bufferSize;
  }

  /**
   * Return the number of files that binding data is split into on internal stage. Used for testing
   * purposes.
   *
   * @return number of files that binding data is split into on internal stage
   */
  public int getFileCount() {
    return this.fileCount;
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
