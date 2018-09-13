/*
 * Copyright (c) 2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.bind;

import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.SFBaseResultSet;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class BindUploader implements Closeable
{
  private static final SFLogger logger = SFLoggerFactory.getLogger(BindUploader.class);

  private static final String PREFIX = "binding_";

  private static final String STAGE_NAME = "SYSTEM$BIND";

  // Date binds are saved as millis, timestamp binds are saved as nanos (and times are not supported)
  private static final String DATE_FORMAT = "ES3";
  private static final String TS_FORMAT = "ES9";

  private static final String CREATE_STAGE_STMT = "CREATE TEMPORARY STAGE "
      + STAGE_NAME
      + " file_format=("
        + " type=csv"
        + " date_format=" + DATE_FORMAT
        + " timestamp_format=" + TS_FORMAT
        + " field_optionally_enclosed_by='\"'"
      + ")";

  private static final String PUT_STMT = "PUT"
      + " 'file://%s%s*'"           // argument 1: folder, argument 2: separator
      + " '%s'"                     // argument 3: stage path
      + " parallel=10"              // upload chunks in parallel
      + " overwrite=true"           // skip file existence check
      + " auto_compress=false"      // we compress already
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

  /**
   * Create a new BindUploader which will write binds to the *existing*
   * bindDir and upload them to the given stageDir
   * @param session the session to use for uploading binds
   * @param stageDir the stage path to upload to
   * @param bindDir the local directory to serialize binds to
   */
  private BindUploader(SFSession session, String stageDir, Path bindDir)
  {
    this.session = session;
    this.stagePath = "@" + STAGE_NAME + "/" + stageDir;
    this.bindDir = bindDir;
  }

  /**
   * Create a new BindUploader which will upload to the given stage path
   * Ensure temporary directory for file writing exists
   * @param session the session to use for uploading binds
   * @param stageDir the stage path to upload to
   * @return BindUploader instance
   * @throws BindException if temporary directory could not be created
   */
  public synchronized static BindUploader newInstance(SFSession session, String stageDir)
      throws BindException
  {
    try
    {
      Path bindDir = Files.createTempDirectory(PREFIX);
      return new BindUploader(session, stageDir, bindDir);
    }
    catch(IOException ex)
    {
      throw new BindException(
          String.format("Failed to create temporary directory: %s", ex.getMessage()), BindException.Type.OTHER);
    }
  }

  /**
   * Upload the bindValues to stage
   * @param bindValues the bind map to upload
   * @throws BindException if the bind map could not be serialized or upload
   *  fails
   */
  public void upload(Map<String, ParameterBindingDTO> bindValues) throws BindException
  {
    if (!closed)
    {
      serializeBinds(bindValues);
      putBinds();
    }
  }

  /**
   * Save the binds to disk
   * @param bindValues the bind map to serialize
   * @throws BindException if bind map improperly formed or writing binds fails
   */
  private void serializeBinds(Map<String, ParameterBindingDTO> bindValues) throws BindException
  {
    List<List<String>> columns = getColumnValues(bindValues);
    List<String[]> rows = buildRows(columns);
    writeRowsToCSV(rows);
  }

  /**
   * Convert bind map to a list of values for each column
   * Perform necessary type casts and invariant checks
   * @param bindValues the bind map to convert
   * @return list of values for each column
   * @throws BindException if bind map is improperly formed
   */
  private List<List<String>> getColumnValues(Map<String, ParameterBindingDTO> bindValues) throws BindException
  {
    List<List<String>> columns = new ArrayList<>(bindValues.size());
    for (int i = 1; i <= bindValues.size(); i++)
    {
      // bindValues should have n entries with string keys 1 ... n and list values
      String key = Integer.toString(i);
      if (!bindValues.containsKey(key))
      {
        throw new BindException(
            String.format("Bind map with %d columns should contain key \"%d\"", bindValues.size(), i), BindException.Type.SERIALIZATION);
      }

      ParameterBindingDTO value = bindValues.get(key);
      try
      {
        List<String> list = (List<String>) value.getValue();
        columns.add(i-1, list);
      }
      catch (ClassCastException ex)
      {
        throw new BindException("Value in binding DTO could not be cast to a list", BindException.Type.SERIALIZATION);
      }
    }
    return columns;
  }

  /**
   * Transpose a list of columns and their values to a list of rows
   * @param columns the list of columns to transpose
   * @return list of rows
   * @throws BindException if columns improperly formed
   */
  private List<String[]> buildRows(List<List<String>> columns) throws BindException
  {
    List<String[]> rows = new ArrayList<>();

    int numColumns = columns.size();
    // columns should have binds
    if (columns.get(0).isEmpty())
    {
      throw new BindException("No binds found in first column", BindException.Type.SERIALIZATION);
    }

    int numRows = columns.get(0).size();
    // every column should have the same number of binds
    for (int i = 0; i < numColumns; i++)
    {
      int iNumRows = columns.get(i).size();
      if (columns.get(i).size() != numRows)
      {
        throw new BindException(
            String.format("Column %d has a different number of binds (%d) than column 1 (%d)", i, iNumRows, numRows), BindException.Type.SERIALIZATION);
      }
    }

    for (int rowIdx = 0; rowIdx < numRows; rowIdx++)
    {
      String[] row = new String[numColumns];
      for (int colIdx = 0; colIdx < numColumns; colIdx++)
      {
        row[colIdx] = columns.get(colIdx).get(rowIdx);
      }
      rows.add(row);
    }

    return rows;
  }

  /**
   * Write the list of rows to compressed CSV files in the temporary directory
   * @param rows the list of rows to write out to a file
   * @throws BindException if exception occurs while writing rows out
   */
  private void writeRowsToCSV(List<String[]> rows) throws BindException
  {
    int numBytes = 0;
    int rowNum = 0;
    int fileCount = 0;

    while(rowNum < rows.size())
    {
      File file = getFile(++fileCount);

      try (OutputStream out = openFile(file))
      {
        // until we reach the last row or the file is too big, write to the file
        numBytes = 0;
        while(numBytes < fileSize && rowNum < rows.size())
        {
          byte[] csv = createCSVRecord(rows.get(rowNum));
          numBytes += csv.length;
          out.write(csv);
          rowNum++;
        }
      }
      catch (IOException ex)
      {
        throw new BindException(
            String.format("Exception encountered while writing to file: %s", ex.getMessage()), BindException.Type.SERIALIZATION);
      }
    }
  }

  /**
   * Create a File object for the given fileNum under the temporary directory
   * @param fileNum the number to use as the file name
   * @return
   */
  private File getFile(int fileNum)
  {
    return bindDir.resolve(Integer.toString(fileNum)).toFile();
  }

  /**
   * Create a new output stream for the given file
   * @param file the file to write out to
   * @return output stream
   * @throws BindException
   */
  private OutputStream openFile(File file) throws BindException
  {
    try
    {
      return new GZIPOutputStream(new FileOutputStream(file));
    }
    catch (IOException ex)
    {
      throw new BindException(
          String.format("Failed to create output file %s: %s", file.toString(), ex.getMessage()), BindException.Type.SERIALIZATION);
    }
  }

  /**
   * Serialize row to a csv
   * Duplicated from StreamLoader class
   * @param data the row to create a csv record from
   * @return serialized csv for row
   */
  private byte[] createCSVRecord(String[] data)
  {
    StringBuilder sb = new StringBuilder(1024);

    for (int i = 0; i < data.length; ++i)
    {
      if(i > 0) sb.append(',');
      sb.append(SnowflakeType.escapeForCSV(data[i]));
    }
    sb.append('\n');
    return sb.toString().getBytes(UTF_8);
  }

  /**
   * Build PUT statement string. Handle filesystem differences and escaping backslashes.
   * @param bindDir the local directory which contains files with binds
   * @param stagePath the stage path to upload to
   * @return put statement for files in bindDir to stagePath
   */
  private String getPutStmt(String bindDir, String stagePath)
  {
    return String.format(PUT_STMT, bindDir, File.separator, stagePath)
        .replaceAll("\\\\", "\\\\\\\\");
  }

  /**
   * Upload binds from local file to stage
   * @throws BindException if uploading the binds fails
   */
  private void putBinds() throws BindException
  {
    createStageIfNeeded();

    String putStatement = getPutStmt(bindDir.toString(), stagePath);

    for (int i = 0; i < PUT_RETRY_COUNT; i++)
    {
      try
      {
        SFStatement statement = new SFStatement(session);
        SFBaseResultSet putResult = statement.execute(putStatement, null, null);
        putResult.next();

        // metadata is 0-based, result set is 1-based
        int column = putResult.getMetaData().getColumnIndex(
            SnowflakeFileTransferAgent.UploadColumns.status.name()) + 1;
        String status = putResult.getString(column);

        if (SnowflakeFileTransferAgent.ResultStatus.UPLOADED.name().equals(status))
        {
          return; // success!
        }
        logger.warn("PUT statement failed. The response had status %s.", status);
      }
      catch (SFException | SQLException ex)
      {
        logger.warn("Exception encountered during PUT operation. ", ex);
      }
    }

    // if we haven't returned (on success), throw exception
    throw new BindException("Failed to PUT files to stage.", BindException.Type.UPLOAD);
  }

  /**
   * Check whether the session's temporary stage has been created, and create it
   * if not.
   * @throws BindException if creating the stage fails
   */
  private void createStageIfNeeded() throws BindException
  {
    if (session.getArrayBindStage() != null)
    {
      return;
    }
    synchronized(session)
    {
      // another thread may have created the session by the time we enter this block
      if (session.getArrayBindStage() == null)
      {
        try
        {
          SFStatement statement = new SFStatement(session);
          statement.execute(CREATE_STAGE_STMT, null, null);
          session.setArrayBindStage(STAGE_NAME);
        }
        catch (SFException | SQLException ex)
        {
          // to avoid repeated failures to create stage, disable array bind stage
          // optimization if we fail to create stage for some reason
          session.setArrayBindStageThreshold(0);
          throw new BindException(
              String.format("Failed to create temporary stage for array binds. %s", ex.getMessage()),BindException.Type.UPLOAD);
        }
      }
    }
  }

  /**
   * Close uploader, deleting the local temporary directory
   *
   * This class can be used in a try-with-resources statement, which ensures that
   * the temporary directory is cleaned up even when exceptions occur
   */
  @Override
  public void close()
  {
    if (!closed)
    {
      try
      {
        if (Files.isDirectory(bindDir))
        {
          for (String fileName : bindDir.toFile().list())
          {
            Files.delete(bindDir.resolve(fileName));
          }
          Files.delete(bindDir);
        }
      }
      catch (IOException ex)
      {
        logger.warn("Exception encountered while trying to clean local directory. ", ex);
      }
      finally
      {
        closed = true;
      }
    }
  }

  /**
   * Set the approximate maximum size in bytes for a single bind file
   * @param fileSize size in bytes
   */
  public void setFileSize(int fileSize)
  {
    this.fileSize = fileSize;
  }

  /**
   * Return the stage path to which binds are uploaded
   * @return the stage path
   */
  public String getStagePath()
  {
    return this.stagePath;
  }

  /**
   * Return the local path to which binds are serialized
   * @return the local path
   */
  public Path getBindDir()
  {
    return this.bindDir;
  }

  /**
   * Compute the number of array bind values in the given bind map
   *
   * @param bindValues the bind map
   * @return 0 if bindValues is null, has no binds, or is not an array bind
   *         n otherwise, where n is the number of binds in the array bind
   */
  public static int arrayBindValueCount(Map<String, ParameterBindingDTO> bindValues)
  {
    if(!isArrayBind(bindValues))
    {
      return 0;
    }
    else
    {
      ParameterBindingDTO bindSample = bindValues.values().iterator().next();
      List<String> bindSampleValues = (List<String>) bindSample.getValue();
      return bindValues.size() * bindSampleValues.size();
    }
  }

  /**
   * Return whether the bind map uses array binds
   * @param bindValues the bind map
   * @return whether the bind map uses array binds
   */
  public static boolean isArrayBind(Map<String, ParameterBindingDTO> bindValues)
  {
    if (bindValues == null || bindValues.size() == 0)
    {
      return false;
    }
    ParameterBindingDTO bindSample = bindValues.values().iterator().next();
    return bindSample.getValue() instanceof List;
  }
}
