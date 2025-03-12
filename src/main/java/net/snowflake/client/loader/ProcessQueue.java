package net.snowflake.client.loader;

import static java.lang.Math.toIntExact;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * This class is responsible for processing a collection of uploaded data files represented by
 * BufferStage class
 */
public class ProcessQueue implements Runnable {
  private static final SFLogger logger = SFLoggerFactory.getLogger(ProcessQueue.class);

  private final Thread _thread;

  private final StreamLoader _loader;

  public ProcessQueue(StreamLoader loader) {
    logger.debug("", false);

    _loader = loader;
    _thread = new Thread(this);
    _thread.setName("ProcessQueueThread");
    _thread.start();
  }

  @Override
  public void run() {

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
        String remoteStage = "@" + _loader.getRemoteStage() + "/" + stage.getRemoteLocation();
        // process uploaded files
        // Loader.abort() and finish() are also synchronized on this
        synchronized (_loader) {
          String updateKeys = getOn(_loader.getKeys(), "T", "S");
          if (stage.getOp() != Operation.INSERT && updateKeys.isEmpty()) {
            _loader.abort(new RuntimeException("No update key column is specified for the job."));
          }

          if (_loader.isAborted()) {
            if (!_loader._preserveStageFile) {
              currentCommand = "RM '" + remoteStage + "'";
              logger.debug(currentCommand, true);
              conn.createStatement().execute(currentCommand);
            } else {
              logger.debug(
                  "Error occurred. The remote stage is preserved for "
                      + "further investigation: {}",
                  remoteStage);
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

          long loaded = 0;
          long parsed = 0;
          int errorCount = 0;
          String lastErrorRow = "";

          // Create temp table to load data (may have a subset of columns)
          logger.debug("Creating Temporary Table: name={}", stage.getId());
          currentState = State.CREATE_TEMP_TABLE;
          List<String> allColumns = getAllColumns(conn);

          // use like to make sure columns in temporary table
          // contains properties (e.g., NOT NULL) from the source table
          currentCommand =
              "CREATE TEMPORARY TABLE \"" + stage.getId() + "\" LIKE " + _loader.getFullTableName();
          List<String> selectedColumns = _loader.getColumns();
          conn.createStatement().execute(currentCommand);

          // In case clustering key exists, drop it from the temporary table so that unused
          // columns can be dropped from the table without errors.
          String dropClusteringKey = "alter table \"" + stage.getId() + "\" drop clustering key";
          conn.createStatement().execute(dropClusteringKey);

          // the temp table can contain only a subset of columns
          // so remove unselected columns
          for (String col : allColumns) {
            if (!selectedColumns.contains(col)) {
              String dropUnSelectedColumn =
                  "alter table \"" + stage.getId() + "\" drop column \"" + col + "\"";
              conn.createStatement().execute(dropUnSelectedColumn);
            }
          }

          // Load data there
          logger.debug(
              "COPY data in the stage to table:" + " stage={}," + " name={}",
              remoteStage,
              stage.getId());
          currentState = State.COPY_INTO_TABLE;
          currentCommand =
              "COPY INTO \""
                  + stage.getId()
                  + "\" FROM '"
                  + remoteStage
                  + "' on_error='"
                  + _loader._onError
                  + "'"
                  + " file_format=("
                  + " field_optionally_enclosed_by='\"'"
                  + " empty_field_as_null="
                  + Boolean.toString(!_loader._copyEmptyFieldAsEmpty)
                  + ")";
          ResultSet rs = conn.createStatement().executeQuery(currentCommand);

          while (rs.next()) {
            // Get the number of rows actually loaded
            loaded += rs.getLong("rows_loaded");
            // Get the number of rows parsed
            parsed += rs.getLong("rows_parsed");
          }

          int errorRecordCount = toIntExact(parsed - loaded);
          logger.debug(
              "errorRecordCount=[{}]," + " parsed=[{}]," + " loaded=[{}]",
              errorRecordCount,
              parsed,
              loaded);

          LoadResultListener listener = _loader.getListener();
          listener.addErrorRecordCount(errorRecordCount);

          if (loaded == stage.getRowCount()) {
            // successfully loaded everything
            logger.debug(
                "COPY command successfully finished:" + " stage={}," + " name={}",
                remoteStage,
                stage.getId());
            listener.addErrorCount(0);
          } else {
            logger.debug(
                "Found errors in COPY command:" + " stage={}," + " name={}",
                remoteStage,
                stage.getId());
            if (listener.needErrors()) {
              currentState = State.COPY_INTO_TABLE_ERROR;
              currentCommand =
                  "COPY INTO \""
                      + stage.getId()
                      + "\" FROM '"
                      + remoteStage
                      + "' validation_mode='return_all_errors'"
                      + " file_format=("
                      + "field_optionally_enclosed_by='\"'"
                      + "empty_field_as_null="
                      + Boolean.toString(!_loader._copyEmptyFieldAsEmpty)
                      + ")";
              ResultSet errorsSet = conn.createStatement().executeQuery(currentCommand);

              Loader.DataError dataError = null;

              while (errorsSet.next()) {
                errorCount++;
                String rn = errorsSet.getString(LoadingError.ErrorProperty.ROW_NUMBER.name());
                if (rn != null && !lastErrorRow.equals(rn)) {
                  // de-duping records with multiple errors
                  lastErrorRow = rn;
                }
                LoadingError loadError = new LoadingError(errorsSet, stage, _loader);

                listener.addError(loadError);
                if (dataError == null) {
                  dataError = loadError.getException();
                }
              }
              logger.debug("errorCount: {}", errorCount);

              listener.addErrorCount(errorCount);
              if (listener.throwOnError()) {
                // stop operation and raise the error
                _loader.abort(dataError);

                if (!_loader._preserveStageFile) {
                  logger.debug("RM: {}", remoteStage);
                  conn.createStatement().execute("RM '" + remoteStage + "'");
                } else {
                  logger.error(
                      "Error occurred. The remote stage is preserved for "
                          + "further investigation: {}",
                      remoteStage);
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
          if (stage.getOp() != Operation.INSERT && stage.getOp() != Operation.DELETE) {

            setStatement = new StringBuilder(" ");
            valueStatement = new StringBuilder("(");

            for (int c = 0; c < _loader.getColumns().size(); ++c) {
              String column = _loader.getColumns().get(c);
              if (c > 0) {
                setStatement.append(", ");
                valueStatement.append(" , ");
              }
              setStatement
                  .append("T.\"")
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
          switch (stage.getOp()) {
            case INSERT:
              {
                loadStatement =
                    "INSERT INTO "
                        + _loader.getFullTableName()
                        + "("
                        + _loader.getColumnsAsString()
                        + ")"
                        + " SELECT * FROM \""
                        + stage.getId()
                        + "\"";
                break;
              }
            case DELETE:
              {
                loadStatement =
                    "DELETE FROM "
                        + _loader.getFullTableName()
                        + " T USING \""
                        + stage.getId()
                        + "\" AS S WHERE "
                        + updateKeys;
                break;
              }
            case MODIFY:
              {
                loadStatement =
                    "MERGE INTO "
                        + _loader.getFullTableName()
                        + " T USING \""
                        + stage.getId()
                        + "\" AS S ON "
                        + updateKeys
                        + " WHEN MATCHED THEN UPDATE SET "
                        + setStatement;
                break;
              }
            case UPSERT:
              {
                loadStatement =
                    "MERGE INTO "
                        + _loader.getFullTableName()
                        + " T USING \""
                        + stage.getId()
                        + "\" AS S ON "
                        + updateKeys
                        + " WHEN MATCHED THEN UPDATE SET "
                        + setStatement
                        + " WHEN NOT MATCHED THEN INSERT("
                        + _loader.getColumnsAsString()
                        + ") VALUES"
                        + valueStatement;
                break;
              }
            default:
              loadStatement = "";
          }
          currentCommand = loadStatement;

          logger.debug("Load Statement: {}", loadStatement);
          Statement s = conn.createStatement();
          s.execute(loadStatement);

          stage.setState(BufferStage.State.PROCESSED);
          currentState = State.FINISH;
          currentCommand = null;
          switch (stage.getOp()) {
            case INSERT:
            case UPSERT:
              {
                _loader.getListener().addProcessedRecordCount(stage.getOp(), stage.getRowCount());

                _loader.getListener().addOperationRecordCount(stage.getOp(), s.getUpdateCount());
                break;
              }
            case DELETE:
            case MODIFY:
              {
                // the number of successful DELETE is the number
                // of processed rows and not the number of given
                // rows.
                _loader.getListener().addProcessedRecordCount(stage.getOp(), s.getUpdateCount());

                _loader.getListener().addOperationRecordCount(stage.getOp(), s.getUpdateCount());
                break;
              }
          }

          // delete stage file if all success
          conn.createStatement().execute("RM '" + remoteStage + "'");

          if (stage.isTerminate()) {
            break;
          }
        }
      } catch (InterruptedException ex) {
        logger.error("Interrupted", ex);
        break;
      } catch (Exception ex) {
        String msg =
            String.format("State: %s, %s, %s", currentState, currentCommand, ex.getMessage());
        _loader.abort(new Loader.ConnectionError(msg, Utils.getCause(ex)));
        logger.error(msg, true);
        if (stage == null || stage.isTerminate()) {
          break;
        }
      }
    }
  }

  private List<String> getAllColumns(final Connection conn) throws SQLException {
    List<String> columns = new LinkedList<>();
    ResultSet result =
        conn.createStatement()
            .executeQuery("show " + "columns" + " in " + _loader.getFullTableName());
    while (result.next()) {
      String col = result.getString("column_name");
      columns.add(col);
    }
    return columns;
  }

  private String getOn(List<String> keys, String L, String R) {
    if (keys == null) {
      return "";
    }
    // L and R don't need to be quoted.
    StringBuilder sb = keys.size() > 1 ? new StringBuilder(64) : new StringBuilder();
    for (int i = 0; i < keys.size(); i++) {
      if (i > 0) {
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

  public void join() {
    logger.trace("Joining threads", false);
    try {
      _thread.join(0);
    } catch (InterruptedException ex) {
      logger.debug("Exception: ", ex);
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
