/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.*;
import java.lang.reflect.Field;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;
import net.snowflake.common.util.ClassUtil;
import net.snowflake.common.util.FixedViewColumn;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;

/** @author jhuang */
public class SnowflakeUtil {

  static final SFLogger logger = SFLoggerFactory.getLogger(RestRequest.class);

  /** Additional data types not covered by standard JDBC */
  public static final int EXTRA_TYPES_TIMESTAMP_LTZ = 50000;

  public static final int EXTRA_TYPES_TIMESTAMP_TZ = 50001;

  // reauthenticate
  private static final int ID_TOKEN_EXPIRED_GS_CODE = 390110;
  private static final int SESSION_NOT_EXIST_GS_CODE = 390111;
  private static final int MASTER_TOKEN_NOTFOUND = 390113;
  private static final int MASTER_EXPIRED_GS_CODE = 390114;
  private static final int MASTER_TOKEN_INVALID_GS_CODE = 390115;
  private static final int ID_TOKEN_INVALID_LOGIN_REQUEST_GS_CODE = 390195;

  public static final String BIG_DECIMAL_STR = "big decimal";
  public static final String FLOAT_STR = "float";
  public static final String DOUBLE_STR = "double";
  public static final String BOOLEAN_STR = "boolean";
  public static final String SHORT_STR = "short";
  public static final String INT_STR = "int";
  public static final String LONG_STR = "long";
  public static final String TIME_STR = "time";
  public static final String TIMESTAMP_STR = "timestamp";
  public static final String DATE_STR = "date";
  public static final String BYTE_STR = "byte";
  public static final String BYTES_STR = "byte array";

  public static void checkErrorAndThrowExceptionIncludingReauth(JsonNode rootNode)
      throws SnowflakeSQLException {
    checkErrorAndThrowExceptionSub(rootNode, true);
  }

  public static void checkErrorAndThrowException(JsonNode rootNode) throws SnowflakeSQLException {
    checkErrorAndThrowExceptionSub(rootNode, false);
  }

  /**
   * Check the error in the JSON node and generate an exception based on information extracted from
   * the node.
   *
   * @param rootNode json object contains error information
   * @param raiseReauthenticateError raises SnowflakeReauthenticationRequest if true
   * @throws SnowflakeSQLException the exception get from the error in the json
   */
  private static void checkErrorAndThrowExceptionSub(
      JsonNode rootNode, boolean raiseReauthenticateError) throws SnowflakeSQLException {
    // no need to throw exception if success
    if (rootNode.path("success").asBoolean()) {
      return;
    }

    String errorMessage;
    String sqlState;
    int errorCode;
    String queryId = "unknown";

    // if we have sqlstate in data, it's a sql error
    if (!rootNode.path("data").path("sqlState").isMissingNode()) {
      sqlState = rootNode.path("data").path("sqlState").asText();
      errorCode = rootNode.path("data").path("errorCode").asInt();
      queryId = rootNode.path("data").path("queryId").asText();
      errorMessage = rootNode.path("message").asText();
    } else {
      sqlState = SqlState.INTERNAL_ERROR; // use internal error sql state

      // check if there is an error code in the envelope
      if (!rootNode.path("code").isMissingNode()) {
        errorCode = rootNode.path("code").asInt();
        errorMessage = rootNode.path("message").asText();
      } else {
        errorCode = ErrorCode.INTERNAL_ERROR.getMessageCode();
        errorMessage = "no_error_code_from_server";

        try {
          PrintWriter writer = new PrintWriter("output.json", "UTF-8");
          writer.print(rootNode.toString());
        } catch (Exception ex) {
          logger.debug("{}", ex);
        }
      }
    }

    if (raiseReauthenticateError) {
      switch (errorCode) {
        case ID_TOKEN_EXPIRED_GS_CODE:
        case SESSION_NOT_EXIST_GS_CODE:
        case MASTER_TOKEN_NOTFOUND:
        case MASTER_EXPIRED_GS_CODE:
        case MASTER_TOKEN_INVALID_GS_CODE:
        case ID_TOKEN_INVALID_LOGIN_REQUEST_GS_CODE:
          throw new SnowflakeReauthenticationRequest(queryId, errorMessage, sqlState, errorCode);
      }
    }
    throw new SnowflakeSQLException(queryId, errorMessage, sqlState, errorCode);
  }

  public static SnowflakeColumnMetadata extractColumnMetadata(
      JsonNode colNode, boolean jdbcTreatDecimalAsInt, SFSession session)
      throws SnowflakeSQLException {
    String colName = colNode.path("name").asText();
    String internalColTypeName = colNode.path("type").asText();
    boolean nullable = colNode.path("nullable").asBoolean();
    int precision = colNode.path("precision").asInt();
    int scale = colNode.path("scale").asInt();
    int length = colNode.path("length").asInt();
    boolean fixed = colNode.path("fixed").asBoolean();
    String extColTypeName;

    int colType;

    SnowflakeType baseType = SnowflakeType.fromString(internalColTypeName);

    switch (baseType) {
      case TEXT:
        colType = Types.VARCHAR;
        extColTypeName = "VARCHAR";
        break;

      case CHAR:
        colType = Types.CHAR;
        extColTypeName = "CHAR";
        break;

      case INTEGER:
        colType = Types.INTEGER;
        extColTypeName = "INTEGER";
        break;

      case FIXED:
        colType = jdbcTreatDecimalAsInt && scale == 0 ? Types.BIGINT : Types.DECIMAL;
        extColTypeName = "NUMBER";
        break;

      case REAL:
        colType = Types.DOUBLE;
        extColTypeName = "DOUBLE";
        break;

      case TIMESTAMP:
      case TIMESTAMP_LTZ:
        colType = EXTRA_TYPES_TIMESTAMP_LTZ;
        extColTypeName = "TIMESTAMPLTZ";
        break;

      case TIMESTAMP_NTZ:
        colType = Types.TIMESTAMP;
        extColTypeName = "TIMESTAMPNTZ";
        break;

      case TIMESTAMP_TZ:
        colType = EXTRA_TYPES_TIMESTAMP_TZ;
        extColTypeName = "TIMESTAMPTZ";
        break;

      case DATE:
        colType = Types.DATE;
        extColTypeName = "DATE";
        break;

      case TIME:
        colType = Types.TIME;
        extColTypeName = "TIME";
        break;

      case BOOLEAN:
        colType = Types.BOOLEAN;
        extColTypeName = "BOOLEAN";
        break;

      case ARRAY:
        colType = Types.VARCHAR;
        extColTypeName = "ARRAY";
        break;

      case OBJECT:
        colType = Types.VARCHAR;
        extColTypeName = "OBJECT";
        break;

      case VARIANT:
        colType = Types.VARCHAR;
        extColTypeName = "VARIANT";
        break;

      case BINARY:
        colType = Types.BINARY;
        extColTypeName = "BINARY";
        break;

      case GEOGRAPHY:
        colType = Types.VARCHAR;
        extColTypeName = "GEOGRAPHY";
        JsonNode udtOutputType = colNode.path("outputType");
        if (!udtOutputType.isMissingNode()) {
          SnowflakeType outputType = SnowflakeType.fromString(udtOutputType.asText());
          switch (outputType) {
            case OBJECT:
            case TEXT:
              colType = Types.VARCHAR;
              break;
            case BINARY:
              colType = Types.BINARY;
          }
        }
        break;

      default:
        throw new SnowflakeSQLLoggedException(
            session,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            SqlState.INTERNAL_ERROR,
            "Unknown column type: " + internalColTypeName);
    }

    JsonNode extColTypeNameNode = colNode.path("extTypeName");
    if (!extColTypeNameNode.isMissingNode()) {
      extColTypeName = extColTypeNameNode.asText();
    }

    String colSrcDatabase = colNode.path("database").asText();
    String colSrcSchema = colNode.path("schema").asText();
    String colSrcTable = colNode.path("table").asText();

    return new SnowflakeColumnMetadata(
        colName,
        colType,
        nullable,
        length,
        precision,
        scale,
        extColTypeName,
        fixed,
        baseType,
        colSrcDatabase,
        colSrcSchema,
        colSrcTable);
  }

  static String javaTypeToSFTypeString(int javaType, SFSession session)
      throws SnowflakeSQLException {
    return SnowflakeType.javaTypeToSFType(javaType, session).name();
  }

  static SnowflakeType javaTypeToSFType(int javaType, SFSession session)
      throws SnowflakeSQLException {
    return SnowflakeType.javaTypeToSFType(javaType, session);
  }

  /**
   * A small function for concatenating two file paths by making sure one and only one path
   * separator is placed between the two paths.
   *
   * <p>This is necessary since for S3 file name, having different number of file separators in a
   * path will mean different files.
   *
   * <p>Typical use case is to concatenate a file name to a directory.
   *
   * @param leftPath left path
   * @param rightPath right path
   * @param fileSep file separator
   * @return concatenated file path
   */
  static String concatFilePathNames(String leftPath, String rightPath, String fileSep) {
    String leftPathTrimmed = leftPath.trim();
    String rightPathTrimmed = rightPath.trim();

    if (leftPathTrimmed.isEmpty()) {
      return rightPath;
    }

    if (leftPathTrimmed.endsWith(fileSep) && rightPathTrimmed.startsWith(fileSep)) {
      return leftPathTrimmed + rightPathTrimmed.substring(1);
    } else if (!leftPathTrimmed.endsWith(fileSep) && !rightPathTrimmed.startsWith(fileSep)) {
      return leftPathTrimmed + fileSep + rightPathTrimmed;
    } else {
      return leftPathTrimmed + rightPathTrimmed;
    }
  }

  static String greatestCommonPrefix(String val1, String val2) {
    if (val1 == null || val2 == null) {
      return null;
    }

    StringBuilder greatestCommonPrefix = new StringBuilder();

    int len = Math.min(val1.length(), val2.length());

    for (int idx = 0; idx < len; idx++) {
      if (val1.charAt(idx) == val2.charAt(idx)) {
        greatestCommonPrefix.append(val1.charAt(idx));
      } else {
        break;
      }
    }

    return greatestCommonPrefix.toString();
  }

  static List<SnowflakeColumnMetadata> describeFixedViewColumns(Class<?> clazz, SFSession session)
      throws SnowflakeSQLException {
    Field[] columns = ClassUtil.getAnnotatedDeclaredFields(clazz, FixedViewColumn.class, true);

    Arrays.sort(columns, new FixedViewColumn.OrdinalComparatorForFields());

    List<SnowflakeColumnMetadata> rowType = new ArrayList<SnowflakeColumnMetadata>();

    for (Field column : columns) {
      FixedViewColumn columnAnnotation = column.getAnnotation(FixedViewColumn.class);

      String typeName;
      int colType;

      Class<?> type = column.getType();
      SnowflakeType stype = SnowflakeType.TEXT;

      if (type == Integer.TYPE) {
        colType = Types.INTEGER;
        typeName = "INTEGER";
        stype = SnowflakeType.INTEGER;
      }
      if (type == Long.TYPE) {
        colType = Types.DECIMAL;
        typeName = "DECIMAL";
        stype = SnowflakeType.INTEGER;
      } else if (type == String.class) {
        colType = Types.VARCHAR;
        typeName = "VARCHAR";
        stype = SnowflakeType.TEXT;
      } else {
        throw new SnowflakeSQLLoggedException(
            session,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            SqlState.INTERNAL_ERROR,
            "Unsupported column type: " + type.getName());
      }

      // TODO: we hard code some of the values below but can change them
      // later to derive from annotation as well.
      rowType.add(
          new SnowflakeColumnMetadata(
              columnAnnotation.name(), // column name
              colType, // column type
              false, // nullable
              20480, // length
              10, // precision
              0, // scale
              typeName, // type name
              true,
              stype, // fixed
              "", // database
              "", // schema
              "")); // table
    }

    return rowType;
  }

  /**
   * A utility to log response details.
   *
   * <p>Used when there is an error in http response
   *
   * @param response http response get from server
   * @param logger logger object
   */
  public static void logResponseDetails(HttpResponse response, SFLogger logger) {
    if (response == null) {
      logger.error("null response");
      return;
    }

    // log the response
    if (response.getStatusLine() != null) {
      logger.error("Response status line reason: {}", response.getStatusLine().getReasonPhrase());
    }

    // log each header from response
    Header[] headers = response.getAllHeaders();
    if (headers != null) {
      for (Header header : headers) {
        logger.debug("Header name: {}, value: {}", header.getName(), header.getValue());
      }
    }

    // log response
    if (response.getEntity() != null) {
      try {
        StringWriter writer = new StringWriter();
        BufferedReader bufferedReader =
            new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
        IOUtils.copy(bufferedReader, writer);
        logger.error("Response content: {}", writer.toString());
      } catch (IOException ex) {
        logger.error("Failed to read content due to exception: " + "{}", ex.getMessage());
      }
    }
  }

  /**
   * Returns a new thread pool configured with the default settings.
   *
   * @param threadNamePrefix prefix of the thread name
   * @param parallel the number of concurrency
   * @return A new thread pool configured with the default settings.
   */
  public static ThreadPoolExecutor createDefaultExecutorService(
      final String threadNamePrefix, final int parallel) {
    ThreadFactory threadFactory =
        new ThreadFactory() {
          private int threadCount = 1;

          public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(threadNamePrefix + threadCount++);
            return thread;
          }
        };
    return (ThreadPoolExecutor) Executors.newFixedThreadPool(parallel, threadFactory);
  }

  public static Throwable getRootCause(Exception ex) {
    Throwable cause = ex;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }

    return cause;
  }

  public static boolean isBlank(String input) {
    if ("".equals(input) || input == null) {
      return true;
    }

    for (char c : input.toCharArray()) {
      if (!Character.isWhitespace(c)) {
        return false;
      }
    }

    return true;
  }

  private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  public static String randomAlphaNumeric(int count) {
    StringBuilder builder = new StringBuilder();
    Random random = new Random();
    while (count-- != 0) {
      int character = random.nextInt(ALPHA_NUMERIC_STRING.length());
      builder.append(ALPHA_NUMERIC_STRING.charAt(character));
    }
    return builder.toString();
  }

  /**
   * System.getProperty wrapper. If System.getProperty raises an SecurityException, it is ignored
   * and returns null.
   *
   * @param property the property name
   * @return the property value if set, otherwise null.
   */
  public static String systemGetProperty(String property) {
    try {
      return System.getProperty(property);
    } catch (SecurityException ex) {
      logger.debug("Security exception raised: {}", ex.getMessage());
      return null;
    }
  }

  /**
   * System.getenv wrapper. If System.getenv raises an SecurityException, it is ignored and returns
   * null.
   *
   * @param env the environment variable name.
   * @return the environment variable value if set, otherwise null.
   */
  public static String systemGetEnv(String env) {
    try {
      return System.getenv(env);
    } catch (SecurityException ex) {
      logger.debug(
          "Failed to get environment variable {}. Security exception raised: {}",
          env,
          ex.getMessage());
    }
    return null;
  }

  /**
   * Setup JDBC proxy properties if necessary.
   *
   * @param info proxy server properties.
   */
  public static void setupProxyPropertiesIfNecessary(Properties info) throws SnowflakeSQLException {
    // Setup proxy properties.
    if (info != null
        && info.size() > 0
        && info.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()) != null) {
      Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>(info.size());
      Boolean useProxy =
          Boolean.valueOf(info.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()));
      if (useProxy) {
        connectionPropertiesMap.put(SFSessionProperty.USE_PROXY, true);

        // set up other proxy related values.
        String propValue = null;
        if ((propValue = info.getProperty(SFSessionProperty.PROXY_HOST.getPropertyKey())) != null) {
          connectionPropertiesMap.put(SFSessionProperty.PROXY_HOST, propValue);
        }
        if ((propValue = info.getProperty(SFSessionProperty.PROXY_PORT.getPropertyKey())) != null) {
          connectionPropertiesMap.put(SFSessionProperty.PROXY_PORT, propValue);
        }
        if ((propValue = info.getProperty(SFSessionProperty.PROXY_USER.getPropertyKey())) != null) {
          connectionPropertiesMap.put(SFSessionProperty.PROXY_USER, propValue);
        }
        if ((propValue = info.getProperty(SFSessionProperty.PROXY_PASSWORD.getPropertyKey()))
            != null) {
          connectionPropertiesMap.put(SFSessionProperty.PROXY_PASSWORD, propValue);
        }
        if ((propValue = info.getProperty(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey()))
            != null) {
          connectionPropertiesMap.put(SFSessionProperty.NON_PROXY_HOSTS, propValue);
        }

        // Setup proxy properties into HttpUtil static cache
        HttpUtil.configureCustomProxyProperties(connectionPropertiesMap);
      }
    }
  }

  /**
   * Round the time value from milliseconds to seconds so the seconds can be used to create
   * SimpleDateFormatter. Negative values have to be rounded to the next negative value, while
   * positive values should be cut off with no rounding.
   *
   * @param millis
   * @return
   */
  public static long getSecondsFromMillis(long millis) {
    long returnVal;
    if (millis < 0) {
      returnVal = (long) Math.ceil((double) Math.abs(millis) / 1000);
      returnVal *= -1;
    } else {
      returnVal = millis / 1000;
    }
    return returnVal;
  }
}
