/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.loader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 *  This class is responsible for processing a collection of uploaded data files
 *  represented by BufferStage class
 */
public class ProcessQueue implements Runnable
{
  private static final SFLogger LOGGER = SFLoggerFactory.getLogger(
          ProcessQueue.class);

  private final Thread _thread;

  private final StreamLoader _loader;

  public ProcessQueue(StreamLoader loader)
  {
    LOGGER.debug("");
    
    _loader = loader;
    _thread = new Thread(this);
    _thread.setName("ProcessQueueThread");
    _thread.start();
  }

  @Override
  public void run()
  {

    while (true) {

      BufferStage stage = null;

      Connection conn = _loader.getProcessConnection();
      State currentState = State.INITIALIZE;
      String currentCommand = null;
      try {
        stage = _loader.takeProcess();

        if (stage.getRowCount() == 0) {
          // Nothing was written to that stage
          if (stage.isTerminate()) {
            break;
          } else {
            continue;
          }
        }

        // Place where the files are.
        // No double quote is added _loader.getRemoteStage(), since
        // it is mostly likely to be "~". If not, we may need to double quote
        // them.
        String remoteStage = "@" + _loader.getRemoteStage() + "/" + stage
                .getRemoteLocation();
        // process uploaded files
        // Loader.abort() and finish() are also synchronized on this
        synchronized (_loader) {

          String updateKeys = getOn(_loader.getKeys(), "T", "S");
          if (stage.getOp() != Operation.INSERT && updateKeys.isEmpty()) {
            _loader.abort(new RuntimeException(
                    "No update key column is specified for the job."));
          }

          if (_loader.isAborted()) {
            if (!_loader._preserveStageFile) {
              currentCommand = "RM '" + remoteStage + "'";
              LOGGER.info(currentCommand);
              conn.createStatement().execute(currentCommand);
            } else {
              LOGGER.warn("Error occurred. The remote stage is preserved for " +
                  "further investigation: {}", remoteStage);
            }
            if (stage.isTerminate()) {
              break;
            } else {
              continue;
            }
            // Do not do anything to this stage.
            // Everything was rolled back upon abort() call
          }
          // Create a temporary table to hold all uploaded data

          int loaded = 0;
          int parsed = 0;
          int errorCount = 0;
          String lastErrorRow = "";

          // Create temp table to load data (may has a subset of columns)
          LOGGER.info("Creating Temporary Table: name={}", stage.getId());
          currentState = State.CREATE_TEMP_TABLE;
          currentCommand = "CREATE TEMPORARY TABLE \""
                  + stage.getId() + "\" AS SELECT "
                  + _loader.getColumnsAsString()
                  + " FROM " + _loader.getFullTableName() + " WHERE FALSE";
          conn.createStatement().execute(currentCommand);

          // Load data there
          LOGGER.info("Copying data in the stage to table:"
                             + " stage={},"
                             + " name={}", remoteStage, stage.getId());
          currentState = State.COPY_INTO_TABLE;
          currentCommand = "COPY INTO \""
                  + stage.getId()
                  + "\" FROM '" + remoteStage
                  + "' on_error='" + _loader._onError + "'"
                  + " file_format=("
                  + " field_optionally_enclosed_by='\"'"
                  + " empty_field_as_null="
                  + Boolean.toString(!_loader._copyEmptyFieldAsEmpty)
                  + ")";
          ResultSet rs = conn.createStatement().executeQuery(currentCommand);

          while(rs.next()) {
            // Get the number of rows actually loaded
            loaded += rs.getInt("rows_loaded");
            // Get the number of rows parsed
            parsed += rs.getInt("rows_parsed");
          }

          int errorRecordCount = parsed - loaded;
          LOGGER.info("errorRecordCount=[{}],"
                             + " parsed=[{}],"
                             + " loaded=[{}]",
                             errorRecordCount, parsed, loaded);

          LoadResultListener listener = _loader.getListener();
          listener.addErrorRecordCount(errorRecordCount);

          if (loaded == stage.getRowCount())
          {
            // successfully loaded everything
            LOGGER.info("COPY command successfully finished");
            listener.addErrorCount(0);
          }
          else
          {
            LOGGER.info("Found errors in COPY command");
            if (listener.needErrors())
            {
              currentState = State.COPY_INTO_TABLE_ERROR;
              currentCommand = "COPY INTO \"" + stage.getId()
                      + "\" FROM '" + remoteStage
                      + "' validation_mode='return_all_errors'"
                      + " file_format=("
                      + "field_optionally_enclosed_by='\"'"
                      + "empty_field_as_null="
                      + Boolean.toString(!_loader._copyEmptyFieldAsEmpty)
                      + ")";
              ResultSet errorsSet = conn.createStatement()
                      .executeQuery(currentCommand);

              Loader.DataError dataError = null;

              while (errorsSet.next()) {
                errorCount++;
                String rn = errorsSet.getString(
                        LoadingError.ErrorProperty.ROW_NUMBER
                        .name());
                if (rn != null && !lastErrorRow.equals(rn)) {
                  // de-duping records with multiple errors
                  lastErrorRow = rn;
                }
                LoadingError loadError = new LoadingError(
                        errorsSet, stage, _loader);

                listener.addError(loadError);
                if (dataError == null) {
                  dataError = loadError.getException();
                }
              }
              LOGGER.info("errorCount: {}", errorCount);

              listener.addErrorCount(errorCount);
              if (listener.throwOnError()) {
                // stop operation and raise the error
                _loader.abort(dataError);

                if (!_loader._preserveStageFile) {
                  LOGGER.debug("RM: {}", remoteStage);
                  conn.createStatement().execute("RM '" + remoteStage + "'");
                } else {
                  LOGGER.error("Error occurred. The remote stage is preserved for " +
                      "further investigation: {}", remoteStage);
                }
                if (stage.isTerminate()) {
                  break;
                } else {
                  continue;
                }
              }
            }
          }

          stage.setState(BufferStage.State.VALIDATED);

          // Generate set and values statement
          StringBuilder setStatement = null;
          StringBuilder valueStatement = null;
          if (stage.getOp() != Operation.INSERT
              && stage.getOp() != Operation.DELETE) {

            setStatement = new StringBuilder(" ");
            valueStatement = new StringBuilder("(");

            for (int c = 0; c < _loader.getColumns().size(); ++c) {
              String column = _loader.getColumns().get(c);
              if (c > 0) {
                setStatement.append(", ");
                valueStatement.append(" , ");
              }
              setStatement.append("T.\"")
                      .append(column)
                      .append("\"=")
                      .append("S.\"")
                      .append(column)
                      .append("\"");
              valueStatement.append("S.\"").append(column).append("\"");
            }
            valueStatement.append(")");
          }

          // generate statement for processing
          currentState = State.INGEST_DATA;
          String loadStatement;
          switch (stage.getOp())
          {
            case INSERT:
            {
              loadStatement = "INSERT INTO "
                              + _loader.getFullTableName()
                              + "(" + _loader.getColumnsAsString() + ")"
                              + " SELECT * FROM \"" + stage.getId() + "\"";
              break;
            }
            case DELETE:
            {
              loadStatement = "DELETE FROM " + _loader.getFullTableName()
                              + " T USING \""
                              + stage.getId() + "\" AS S WHERE "
                              + updateKeys;
              break;
            }
            case MODIFY:
            {
              loadStatement = "MERGE INTO " + _loader.getFullTableName()
                              + " T USING \""
                              + stage.getId() + "\" AS S ON "
                              + updateKeys
                              + " WHEN MATCHED THEN UPDATE SET " + setStatement;
              break;
            }
            case UPSERT:
            {

              loadStatement = "MERGE INTO " + _loader.getFullTableName()
                              + " T USING \""
                              + stage.getId() + "\" AS S ON "
                              + updateKeys
                              + " WHEN MATCHED THEN UPDATE SET " + setStatement
                              + " WHEN NOT MATCHED THEN INSERT("
                              + _loader.getColumnsAsString() + ") VALUES"
                              + valueStatement;
              break;
            }
            default:
              loadStatement = "";
          }
          currentCommand = loadStatement;

          LOGGER.debug("Load Statement: {}", loadStatement);
          Statement s = conn.createStatement();
          s.execute(loadStatement);
          ResultSet prs = s.getResultSet();
          prs.next();

          stage.setState(BufferStage.State.PROCESSED);
          currentState = State.FINISH;
          currentCommand = null;
          switch (stage.getOp())
          {
            case INSERT:
            {
              _loader.getListener().addProcessedRecordCount(
                      stage.getOp(), stage.getRowCount());

              _loader.getListener().addOperationRecordCount(
                      Operation.INSERT, prs.getInt(1));
              break;
            }
            case DELETE:
            {
              // the number of successful DELETE is the number 
              // of processed rows and not the number of given
              // rows.
              _loader.getListener().addProcessedRecordCount(
                      stage.getOp(), prs.getInt(1));

              _loader.getListener().addOperationRecordCount(
                      Operation.DELETE, prs.getInt(1));
              break;
            }
            case MODIFY:
            {
              // the number of successful UPDATE
              _loader.getListener().addProcessedRecordCount(
                      stage.getOp(), prs.getInt(1));

              _loader.getListener().addOperationRecordCount(
                      Operation.MODIFY, prs.getInt(1));
              break;
            }
            case UPSERT:
            {
              _loader.getListener().addProcessedRecordCount(
                      stage.getOp(), stage.getRowCount());

              _loader.getListener().addOperationRecordCount(
                      Operation.UPSERT, prs.getInt(1) + prs.getInt(2));
              break;
            }

          }

          // delete stage file if all success
          conn.createStatement().execute("RM '" + remoteStage + "'");

          if (stage.isTerminate())
          {
            break;
          }

        }
      }
      catch (InterruptedException ex) {
        LOGGER.error("Interrupted", ex);
        break;
      }
      catch (Exception ex) {
        String msg = String.format("State: %s, %s, %s",
                currentState, currentCommand, ex.getMessage());
        _loader.abort(new Loader.ConnectionError(
                msg, Utils.getCause(ex)));
        LOGGER.error(msg);
        if (stage == null || stage.isTerminate()) {
          break;
        }
      }
    }
  }

  private String getOn(List<String> keys, String L, String R)
  {
    if (keys == null) {
      return "";
    }
    // L and R don't need to be quoted.
    StringBuilder sb = keys.size() > 1 ?
                       new StringBuilder(64) : new StringBuilder();
    for (int i = 0; i < keys.size(); i++)
    {
      if (i > 0)
      {
        sb.append("AND ");
      }
      sb.append(L);
      sb.append(".\"");
      sb.append(keys.get(i));
      sb.append("\" = ");
      sb.append(R);
      sb.append(".\"");
      sb.append(keys.get(i));
      sb.append("\" ");
    }
    return sb.toString();
  }

  public void join()
  {
    LOGGER.debug("");
    try
    {
      _thread.join(0);
    }
    catch (InterruptedException ex)
    {
      LOGGER.debug("Exception: ", ex);
    }
  }

  private enum State {
    INITIALIZE,
    CREATE_TEMP_TABLE,
    COPY_INTO_TABLE,
    COPY_INTO_TABLE_ERROR,
    INGEST_DATA,
    FINISH
  }
}
