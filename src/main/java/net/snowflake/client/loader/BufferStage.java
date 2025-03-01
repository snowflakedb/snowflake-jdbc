package net.snowflake.client.loader;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Class representing a unit of work for uploader. Corresponds to a collection of data files for a
 * single processing stage.
 */
public class BufferStage {
  private static final SFLogger logger = SFLoggerFactory.getLogger(BufferStage.class);

  public enum State {
    CREATED,
    LOADING,
    LOADED,
    EMPTY,
    UPLOADED,
    VALIDATED,
    VALIDATED_CLEANED,
    ERROR,
    PROCESSED,
    CLEANED,
    REMOVED
  };

  public static final int FILE_BUCKET_SIZE = 64; // threshold to schedule processing
  public static final long FILE_SIZE = 50L * 1024L * 1024L; // individual file, 50Mb

  private State _state;

  private final File _directory;

  private final String _location;

  private final String _stamp;

  private final Operation _op;

  private final long _csvFileBucketSize;

  private final long _csvFileSize;

  // Last stage in a loader gets to terminate
  private volatile boolean _terminate = false;

  private String _id;

  // Data bytes (uncompressed) in the current file
  private int _currentSize = 0;

  // Total number of rows submitted to this stage
  private int _rowCount = 0;

  // Number of files in the stage
  private int _fileCount = 0;

  // Counter for ID generation
  private static AtomicLong MARK = new AtomicLong(1);

  // Parent loader
  private StreamLoader _loader;

  // Current output stream
  private OutputStream _outstream = null;

  // Current file
  private File _file = null;

  // List of all scheduled uploaders
  private ArrayList<FileUploader> _uploaders = new ArrayList<>();

  BufferStage(StreamLoader loader, Operation op, long csvFileBucketSize, long csvFileSize) {
    logger.debug("Operation: {}", op);

    _state = State.CREATED;
    _loader = loader;

    _stamp = new SimpleDateFormat("yyyyMMdd'_'HHmmss'_'SSS").format(new Date());
    _csvFileBucketSize = csvFileBucketSize;
    _csvFileSize = csvFileSize;

    long mark = MARK.getAndIncrement() % 10000000;

    // Security Fix: A table name can include slashes and dots, so if a table
    // name is used as part of a file name, the file can be created
    // outside of the given directory. This replaces slashes with underscores.
    _location =
        BufferStage.escapeFileSeparatorChar(_loader.getTable())
            + File.separatorChar
            + op.name()
            + File.separatorChar
            + _stamp
            + "_"
            + _loader.getNoise()
            + '_'
            + mark;

    _id = BufferStage.escapeFileSeparatorChar(_loader.getTable()) + "_" + _stamp + '_' + mark;

    String localStageDirectory = _loader.getBase() + File.separatorChar + _location;

    _directory = new File(localStageDirectory);
    if (!_directory.mkdirs()) {
      RuntimeException ex =
          new RuntimeException(
              "Could not initialize the local staging area. "
                  + "Make sure the directory is writable and readable: "
                  + localStageDirectory);

      _loader.abort(ex);
      throw ex;
    }

    _op = op;

    openFile();
  }

  /** Create local file for caching data before upload */
  private synchronized void openFile() {
    try {
      String fName =
          _directory.getAbsolutePath()
              + File.separatorChar
              + StreamLoader.FILE_PREFIX
              + _stamp
              + _fileCount;
      if (_loader._compressDataBeforePut) {
        fName += StreamLoader.FILE_SUFFIX;
      }
      logger.debug("openFile: {}", fName);

      OutputStream fileStream = new FileOutputStream(fName);
      if (_loader._compressDataBeforePut) {
        OutputStream gzipOutputStream =
            new GZIPOutputStream(fileStream, 64 * 1024, true) {
              {
                def.setLevel((int) _loader._compressLevel);
              }
            };
        _outstream = new BufferedOutputStream(gzipOutputStream);
      } else {
        _outstream = new BufferedOutputStream(fileStream);
      }

      _file = new File(fName);

      _fileCount++;
    } catch (IOException ex) {
      _loader.abort(new Loader.ConnectionError(Utils.getCause(ex)));
    }
  }

  private static byte[] newLineBytes = "\n".getBytes(UTF_8);

  // not thread safe
  boolean stageData(final byte[] line) throws IOException {
    if (this._rowCount % 10000 == 0) {
      logger.debug("rowCount: {}, currentSize: {}", this._rowCount, _currentSize);
    }
    _outstream.write(line);
    _currentSize += line.length;

    _outstream.write(newLineBytes);
    this._rowCount++;

    if (_loader._testRemoteBadCSV) {
      // inject garbage for a negative test case
      // The file will be uploaded to the stage, but COPY command will
      // fail and raise LoaderError
      _outstream.write(new byte[] {(byte) 0x01, (byte) 0x02});
      _outstream.write(newLineBytes);
      this._rowCount++;
    }

    if (_currentSize >= this._csvFileSize) {
      logger.debug(
          "name: {}, currentSize: {}, Threshold: {}," + " fileCount: {}, fileBucketSize: {}",
          _file.getAbsolutePath(),
          _currentSize,
          this._csvFileSize,
          _fileCount,
          this._csvFileBucketSize);
      _outstream.flush();
      _outstream.close();
      _outstream = null;
      FileUploader fu = new FileUploader(_loader, _location, _file);
      fu.upload();
      _uploaders.add(fu);
      openFile();
      _currentSize = 0;
    }

    return _fileCount > this._csvFileBucketSize;
  }

  /**
   * Wait for all files to finish uploading and schedule stage for processing
   *
   * @throws IOException raises an exception if IO error occurs
   */
  void completeUploading() throws IOException {
    logger.debug(
        "name: {}, currentSize: {}, Threshold: {}," + " fileCount: {}, fileBucketSize: {}",
        _file.getAbsolutePath(),
        _currentSize,
        this._csvFileSize,
        _fileCount,
        this._csvFileBucketSize);

    _outstream.flush();
    _outstream.close();
    // last file
    if (_currentSize > 0) {
      FileUploader fu = new FileUploader(_loader, _location, _file);
      fu.upload();
      _uploaders.add(fu);
    } else {
      // delete empty file
      _file.delete();
    }

    for (FileUploader fu : _uploaders) {
      // Finish all files being uploaded
      fu.join();
    }

    // Delete the directory once we are done (for easier tracking
    // of what is going on)
    _directory.deleteOnExit();

    if (this._rowCount == 0) {
      setState(State.EMPTY);
    }
  }

  String getRemoteLocation() {
    return remoteSeparator(_location);
  }

  Operation getOp() {
    return _op;
  }

  public boolean isTerminate() {
    return _terminate;
  }

  public void setTerminate(boolean terminate) {
    this._terminate = terminate;
  }

  public String getId() {
    return _id;
  }

  public void setId(String _id) {
    this._id = _id;
  }

  public State state() {
    return _state;
  }

  public void setState(State state) {
    if (_state != state) {
      // Logging goes here
      // Need to keep trace of states.
      _state = state;
    }
  }

  int getRowCount() {
    return _rowCount;
  }

  // convert any back slashes to forward slashes if necessary when converting
  // a local filename to a one suitable for S3
  private String remoteSeparator(String fname) {
    if (File.separatorChar == '\\') {
      return fname.replace("\\", "/");
    } else {
      return fname;
    }
  }

  /**
   * Escape file separator char to underscore. This prevents the file name from using file path
   * separator.
   *
   * @param fname The file name to escape
   * @return escaped file name
   */
  private static String escapeFileSeparatorChar(String fname) {
    if (File.separatorChar == '\\') {
      return fname.replaceAll(File.separator + File.separator, "_");
    } else {
      return fname.replaceAll(File.separator, "_");
    }
  }
}
