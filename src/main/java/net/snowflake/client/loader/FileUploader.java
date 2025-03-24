package net.snowflake.client.loader;

import java.io.File;
import java.sql.ResultSet;
import java.sql.Statement;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/** Class responsible for uploading a single data file. */
public class FileUploader implements Runnable {
  private static final SFLogger logger = SFLoggerFactory.getLogger(PutQueue.class);

  private static final int RETRY = 6;
  private final Thread _thread;
  private final StreamLoader _loader;
  private final String _stage;
  private final File _file;

  FileUploader(StreamLoader loader, String stage, File file) {
    logger.trace("Creating new FileUploader", false);
    _loader = loader;
    _thread = new Thread(this);
    _thread.setName("FileUploaderThread");
    _stage = stage;
    _file = file;
  }

  public synchronized void upload() {
    // throttle up will wait if too many files are uploading
    logger.trace("Creating new FileUploader", false);
    _loader.throttleUp();
    _thread.start();
  }

  @Override
  public void run() {

    Throwable previousException = null;
    try {
      for (int attempt = 0; attempt <= RETRY; attempt++) {

        if (attempt == RETRY) {
          if (previousException != null) {
            _loader.abort(
                new Loader.ConnectionError(
                    String.format(
                        "File could not be uploaded to remote stage "
                            + "after retrying %d times: %s",
                        RETRY, _file.getCanonicalPath()),
                    Utils.getCause(previousException)));
          } else {
            _loader.abort(
                new Loader.ConnectionError(
                    String.format(
                        "File could not be uploaded to remote stage "
                            + "after retrying %d times: %s",
                        RETRY, _file.getCanonicalPath())));
          }
          break;
        }

        if (attempt > 0) {
          logger.debug("Will retry PUT after {} seconds", Math.pow(2, attempt));
          Thread.sleep(1000 * ((int) Math.pow(2, attempt)));
        }

        // In test mode force fail first file
        if (_loader._testMode) {
          // TEST MODE
          if (attempt < 2) {
            _loader
                .getPutConnection()
                .unwrap(SnowflakeConnectionV1.class)
                .setInjectFileUploadFailure(_file.getName());
          } else {
            // so that retry now succeeds.
            _loader
                .getPutConnection()
                .unwrap(SnowflakeConnectionV1.class)
                .setInjectFileUploadFailure(null);
          }
        }

        // Upload local files to a remote stage

        // No double quote is added _loader.getRemoteStage(), since
        // it is most likely "~". If not, we may need to double quote
        // them.
        String remoteStage = "@" + _loader.getRemoteStage() + "/" + remoteSeparator(_stage);

        String putStatement =
            "PUT "
                + (attempt > 0 ? "/* retry:" + attempt + " */ " : "")
                + "'file://"
                + _file.getCanonicalPath().replaceAll("\\\\", "\\\\\\\\")
                + "' '"
                + remoteStage
                + "' parallel=10" // upload chunks in parallel
                + " overwrite=true"; // skip file existence check
        if (_loader._compressDataBeforePut) {
          putStatement += " auto_compress=false" + " SOURCE_COMPRESSION=gzip";
        } else if (_loader._compressFileByPut) {
          putStatement += " auto_compress=true";
        } else {
          // don't compress file at all
          putStatement += " auto_compress=false";
        }

        Statement statement = _loader.getPutConnection().createStatement();
        try {
          logger.debug("Put Statement start: {}", putStatement);
          statement.execute(putStatement);
          logger.debug("Put Statement end: {}", putStatement);
          ResultSet putResult = statement.getResultSet();

          putResult.next();

          String file =
              localSeparator(
                  putResult.getString(SnowflakeFileTransferAgent.UploadColumns.source.name()));
          String status =
              putResult.getString(SnowflakeFileTransferAgent.UploadColumns.status.name());
          String message =
              putResult.getString(SnowflakeFileTransferAgent.UploadColumns.message.name());

          if (status != null
              && status.equals(SnowflakeFileTransferAgent.ResultStatus.UPLOADED.name())) {
            // UPLOAD is success
            _file.delete();
            break;
          } else {
            // The log level should be WARNING for a single upload failure.
            if (message.startsWith("Simulated upload failure")) {
              logger.debug(
                  "Failed to upload a file:" + " status={}," + " filename={}," + " message={}",
                  status,
                  file,
                  message);
            } else {
              logger.debug(
                  "Failed to upload a file:" + " status={}," + " filename={}," + " message={}",
                  status,
                  file,
                  message);
            }
          }
        } catch (Throwable t) {
          // The log level for unknown error is set to SEVERE
          logger.error(
              String.format(
                  "Failed to PUT on attempt: attempt=[%s], " + "Message=[%s]",
                  attempt, t.getMessage()),
              t.getCause());
          previousException = t;
        }
      }
    } catch (Throwable t) {
      logger.error("PUT exception", t);
      _loader.abort(new Loader.ConnectionError(t.getMessage(), t.getCause()));
    } finally {
      _loader.throttleDown();
    }
  }

  public void join() {
    logger.trace("Joining threads", false);
    try {
      _thread.join(0);
    } catch (InterruptedException ex) {
      logger.error(ex.getMessage(), ex);
    }
  }

  /**
   * convert any back slashes to forward slashes if necessary when converting a local filename to a
   * one suitable for S3
   *
   * @param fname a file name to PUT
   * @return A fname string for S3
   */
  private String remoteSeparator(String fname) {
    if (File.separatorChar == '\\') {
      return fname.replace("\\", "/");
    } else {
      return fname;
    }
  }

  /**
   * convert any forward slashes to back slashes if necessary when converting a S3 file name to a
   * local file name
   *
   * @param fname a file name to PUT
   * @return A fname string for the local FS (Windows/other Unix like OS)
   */
  private String localSeparator(String fname) {
    if (File.separatorChar == '\\') {
      return fname.replace("/", "\\");
    } else {
      return fname;
    }
  }
}
