package net.snowflake.client.jdbc;

import static java.util.Arrays.stream;
import static net.snowflake.client.core.Constants.OAUTH_ACCESS_TOKEN_EXPIRED_GS_CODE;
import static net.snowflake.client.jdbc.SnowflakeType.GEOGRAPHY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import net.snowflake.client.core.Constants;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.ThrowingCallable;
import net.snowflake.common.core.SqlState;
import net.snowflake.common.util.ClassUtil;
import net.snowflake.common.util.FixedViewColumn;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;

public class SnowflakeUtil {

  private static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeUtil.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();

  /** Additional data types not covered by standard JDBC */
  public static final int EXTRA_TYPES_TIMESTAMP_LTZ = 50000;

  public static final int EXTRA_TYPES_TIMESTAMP_TZ = 50001;

  public static final int EXTRA_TYPES_TIMESTAMP_NTZ = 50002;

  public static final int EXTRA_TYPES_VECTOR = 50003;

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

  public static String mapJson(Object ob) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(ob);
  }

  public static void checkErrorAndThrowExceptionIncludingReauth(JsonNode rootNode)
      throws SnowflakeSQLException {
    checkErrorAndThrowExceptionSub(rootNode, true);
  }

  public static void checkErrorAndThrowException(JsonNode rootNode) throws SnowflakeSQLException {
    checkErrorAndThrowExceptionSub(rootNode, false);
  }

  public static long getEpochTimeInMicroSeconds() {
    Instant timestamp = Instant.now();
    long micros =
        TimeUnit.SECONDS.toMicros(timestamp.getEpochSecond())
            + TimeUnit.NANOSECONDS.toMicros(timestamp.getNano());
    return micros;
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

        try (PrintWriter writer = new PrintWriter("output.json", "UTF-8")) {
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
        case OAUTH_ACCESS_TOKEN_EXPIRED_GS_CODE:
          throw new SnowflakeReauthenticationRequest(queryId, errorMessage, sqlState, errorCode);
      }
    }
    throw new SnowflakeSQLException(queryId, errorMessage, sqlState, errorCode);
  }

  /**
   * This method should only be used internally
   *
   * @param colNode JsonNode
   * @param jdbcTreatDecimalAsInt true if should treat Decimal as Int
   * @param session SFBaseSession
   * @return SnowflakeColumnMetadata
   * @throws SnowflakeSQLException if an error occurs
   */
  @Deprecated
  public static SnowflakeColumnMetadata extractColumnMetadata(
      JsonNode colNode, boolean jdbcTreatDecimalAsInt, SFBaseSession session)
      throws SnowflakeSQLException {
    return new SnowflakeColumnMetadata(colNode, jdbcTreatDecimalAsInt, session);
  }

  static ColumnTypeInfo getSnowflakeType(
      String internalColTypeName,
      String extColTypeName,
      JsonNode udtOutputType,
      SFBaseSession session,
      int fixedColType,
      boolean isStructuredType,
      boolean isVectorType)
      throws SnowflakeSQLLoggedException {
    SnowflakeType baseType = SnowflakeType.fromString(internalColTypeName);
    ColumnTypeInfo columnTypeInfo = null;

    switch (baseType) {
      case TEXT:
        columnTypeInfo =
            new ColumnTypeInfo(Types.VARCHAR, defaultIfNull(extColTypeName, "VARCHAR"), baseType);
        break;
      case CHAR:
        columnTypeInfo =
            new ColumnTypeInfo(Types.CHAR, defaultIfNull(extColTypeName, "CHAR"), baseType);
        break;
      case INTEGER:
        columnTypeInfo =
            new ColumnTypeInfo(Types.INTEGER, defaultIfNull(extColTypeName, "INTEGER"), baseType);
        break;
      case FIXED:
        if (isVectorType) {
          columnTypeInfo =
              new ColumnTypeInfo(Types.INTEGER, defaultIfNull(extColTypeName, "INTEGER"), baseType);
        } else {
          columnTypeInfo =
              new ColumnTypeInfo(fixedColType, defaultIfNull(extColTypeName, "NUMBER"), baseType);
        }
        break;

      case REAL:
        if (isVectorType) {
          columnTypeInfo =
              new ColumnTypeInfo(Types.FLOAT, defaultIfNull(extColTypeName, "FLOAT"), baseType);
        } else {
          columnTypeInfo =
              new ColumnTypeInfo(Types.DOUBLE, defaultIfNull(extColTypeName, "DOUBLE"), baseType);
        }
        break;

      case TIMESTAMP:
      case TIMESTAMP_LTZ:
        columnTypeInfo =
            new ColumnTypeInfo(
                EXTRA_TYPES_TIMESTAMP_LTZ, defaultIfNull(extColTypeName, "TIMESTAMPLTZ"), baseType);
        break;

      case TIMESTAMP_NTZ:
        // if the column type is changed to EXTRA_TYPES_TIMESTAMP_NTZ, update also JsonSqlInput
        columnTypeInfo =
            new ColumnTypeInfo(
                Types.TIMESTAMP, defaultIfNull(extColTypeName, "TIMESTAMPNTZ"), baseType);
        break;

      case TIMESTAMP_TZ:
        columnTypeInfo =
            new ColumnTypeInfo(
                EXTRA_TYPES_TIMESTAMP_TZ, defaultIfNull(extColTypeName, "TIMESTAMPTZ"), baseType);
        break;

      case DATE:
        columnTypeInfo =
            new ColumnTypeInfo(Types.DATE, defaultIfNull(extColTypeName, "DATE"), baseType);
        break;

      case TIME:
        columnTypeInfo =
            new ColumnTypeInfo(Types.TIME, defaultIfNull(extColTypeName, "TIME"), baseType);
        break;

      case BOOLEAN:
        columnTypeInfo =
            new ColumnTypeInfo(Types.BOOLEAN, defaultIfNull(extColTypeName, "BOOLEAN"), baseType);
        break;

      case VECTOR:
        columnTypeInfo =
            new ColumnTypeInfo(
                EXTRA_TYPES_VECTOR, defaultIfNull(extColTypeName, "VECTOR"), baseType);
        break;

      case ARRAY:
        int columnType = isStructuredType ? Types.ARRAY : Types.VARCHAR;
        columnTypeInfo =
            new ColumnTypeInfo(columnType, defaultIfNull(extColTypeName, "ARRAY"), baseType);
        break;

      case MAP:
        columnTypeInfo =
            new ColumnTypeInfo(Types.STRUCT, defaultIfNull(extColTypeName, "OBJECT"), baseType);
        break;

      case OBJECT:
        if (isStructuredType) {
          boolean isGeoType =
              "GEOMETRY".equals(extColTypeName) || "GEOGRAPHY".equals(extColTypeName);
          int type = isGeoType ? Types.VARCHAR : Types.STRUCT;
          columnTypeInfo =
              new ColumnTypeInfo(type, defaultIfNull(extColTypeName, "OBJECT"), baseType);
        } else {
          columnTypeInfo =
              new ColumnTypeInfo(Types.VARCHAR, defaultIfNull(extColTypeName, "OBJECT"), baseType);
        }
        break;

      case VARIANT:
        columnTypeInfo =
            new ColumnTypeInfo(Types.VARCHAR, defaultIfNull(extColTypeName, "VARIANT"), baseType);
        break;

      case BINARY:
        columnTypeInfo =
            new ColumnTypeInfo(Types.BINARY, defaultIfNull(extColTypeName, "BINARY"), baseType);
        break;

      case GEOGRAPHY:
      case GEOMETRY:
        int colType = Types.VARCHAR;
        extColTypeName = (baseType == GEOGRAPHY) ? "GEOGRAPHY" : "GEOMETRY";

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
        columnTypeInfo = new ColumnTypeInfo(colType, extColTypeName, baseType);
        break;

      default:
        throw new SnowflakeSQLLoggedException(
            session,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            SqlState.INTERNAL_ERROR,
            "Unknown column type: " + internalColTypeName);
    }

    return columnTypeInfo;
  }

  private static String defaultIfNull(String extColTypeName, String defaultValue) {
    return Optional.ofNullable(extColTypeName).orElse(defaultValue);
  }

  static List<FieldMetadata> createFieldsMetadata(
      ArrayNode fieldsJson, boolean jdbcTreatDecimalAsInt, String parentInternalColumnTypeName)
      throws SnowflakeSQLLoggedException {
    List<FieldMetadata> fields = new ArrayList<>();
    for (JsonNode node : fieldsJson) {
      String colName;
      if (!node.path("fieldType").isEmpty()) {
        colName = node.path("fieldName").asText();
        node = node.path("fieldType");
      } else {
        colName = node.path("name").asText();
      }
      int scale = node.path("scale").asInt();
      int precision = node.path("precision").asInt();
      String internalColTypeName = node.path("type").asText();
      boolean nullable = node.path("nullable").asBoolean();
      int length = node.path("length").asInt();
      boolean fixed = node.path("fixed").asBoolean();
      int fixedColType = jdbcTreatDecimalAsInt && scale == 0 ? Types.BIGINT : Types.DECIMAL;
      List<FieldMetadata> internalFields =
          getFieldMetadata(jdbcTreatDecimalAsInt, parentInternalColumnTypeName, node);
      JsonNode outputType = node.path("outputType");
      JsonNode extColTypeNameNode = node.path("extTypeName");
      String extColTypeName = null;
      if (!extColTypeNameNode.isMissingNode() && !isNullOrEmpty(extColTypeNameNode.asText())) {
        extColTypeName = extColTypeNameNode.asText();
      }
      ColumnTypeInfo columnTypeInfo =
          getSnowflakeType(
              internalColTypeName,
              extColTypeName,
              outputType,
              null,
              fixedColType,
              internalFields.size() > 0,
              isVectorType(parentInternalColumnTypeName));
      fields.add(
          new FieldMetadata(
              colName,
              columnTypeInfo.getExtColTypeName(),
              columnTypeInfo.getColumnType(),
              nullable,
              length,
              precision,
              scale,
              fixed,
              columnTypeInfo.getSnowflakeType(),
              internalFields));
    }
    return fields;
  }

  static boolean isVectorType(String internalColumnTypeName) {
    return internalColumnTypeName.equalsIgnoreCase("vector");
  }

  static List<FieldMetadata> getFieldMetadata(
      boolean jdbcTreatDecimalAsInt, String internalColumnTypeName, JsonNode node)
      throws SnowflakeSQLLoggedException {
    if (!node.path("fields").isEmpty()) {
      ArrayNode internalFieldsJson = (ArrayNode) node.path("fields");
      return createFieldsMetadata(
          internalFieldsJson, jdbcTreatDecimalAsInt, internalColumnTypeName);
    } else {
      return new ArrayList<>();
    }
  }

  static String javaTypeToSFTypeString(int javaType, SFBaseSession session)
      throws SnowflakeSQLException {
    return SnowflakeType.javaTypeToSFType(javaType, session).name();
  }

  @SnowflakeJdbcInternalApi
  public static SnowflakeType javaTypeToSFType(int javaType, SFBaseSession session)
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

  static List<SnowflakeColumnMetadata> describeFixedViewColumns(
      Class<?> clazz, SFBaseSession session) throws SnowflakeSQLException {
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
              new ArrayList<>(),
              "", // database
              "", // schema
              "",
              false, // isAutoincrement
              0 // dimension
              ));
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
      logger.error("null response", false);
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
   * System.setEnv function. Can be used for unit tests.
   *
   * @param key key
   * @param value value
   */
  public static void systemSetEnv(String key, String value) {
    try {
      Map<String, String> env = System.getenv();
      Class<?> cl = env.getClass();
      Field field = cl.getDeclaredField("m");
      field.setAccessible(true);
      Map<String, String> writableEnv = (Map<String, String>) field.get(env);
      writableEnv.put(key, value);

      // To an environment variable is set on Windows, it uses a different map to store the values
      // when the system.getenv(VAR_NAME) is used its required to update in this additional place.
      if (Constants.getOS() == Constants.OS.WINDOWS) {
        Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
        Method getenv = pe.getDeclaredMethod("getenv", String.class);
        getenv.setAccessible(true);
        Field props = pe.getDeclaredField("theCaseInsensitiveEnvironment");
        props.setAccessible(true);
        Map<String, String> writableEnvForGet = (Map<String, String>) props.get(null);
        writableEnvForGet.put(key, value);
      }
    } catch (Exception e) {
      logger.error(
          "Failed to set environment variable {}. Exception raised: {}", key, e.getMessage());
    }
  }

  /**
   * System.unsetEnv function to remove a system environment parameter in the map
   *
   * @param key key value
   */
  public static void systemUnsetEnv(String key) {
    try {
      Map<String, String> env = System.getenv();
      Class<?> cl = env.getClass();
      Field field = cl.getDeclaredField("m");
      field.setAccessible(true);
      Map<String, String> writableEnv = (Map<String, String>) field.get(env);
      writableEnv.remove(key);
    } catch (Exception e) {
      logger.error(
          "Failed to remove environment variable {}. Exception raised: {}", key, e.getMessage());
    }
  }

  /**
   * Setup JDBC proxy properties if necessary.
   *
   * @param mode OCSP mode
   * @param info proxy server properties.
   * @return HttpClientSettingsKey
   * @throws SnowflakeSQLException if an error occurs
   */
  public static HttpClientSettingsKey convertProxyPropertiesToHttpClientKey(
      OCSPMode mode, Properties info) throws SnowflakeSQLException {
    // Setup proxy properties.
    if (info != null
        && info.size() > 0
        && info.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()) != null) {
      Boolean useProxy =
          Boolean.valueOf(info.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()));
      if (useProxy) {
        // set up other proxy related values.
        String proxyHost = info.getProperty(SFSessionProperty.PROXY_HOST.getPropertyKey());
        int proxyPort;
        try {
          proxyPort =
              Integer.parseInt(info.getProperty(SFSessionProperty.PROXY_PORT.getPropertyKey()));
        } catch (NumberFormatException | NullPointerException e) {
          throw new SnowflakeSQLException(
              ErrorCode.INVALID_PROXY_PROPERTIES, "Could not parse port number");
        }
        String proxyUser = info.getProperty(SFSessionProperty.PROXY_USER.getPropertyKey());
        String proxyPassword = info.getProperty(SFSessionProperty.PROXY_PASSWORD.getPropertyKey());
        String nonProxyHosts = info.getProperty(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey());
        String proxyProtocol = info.getProperty(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey());
        String userAgentSuffix =
            info.getProperty(SFSessionProperty.USER_AGENT_SUFFIX.getPropertyKey());
        Boolean gzipDisabled =
            isNullOrEmpty(info.getProperty(SFSessionProperty.GZIP_DISABLED.getPropertyKey()))
                ? false
                : Boolean.valueOf(
                    info.getProperty(SFSessionProperty.GZIP_DISABLED.getPropertyKey()));

        // create key for proxy properties
        return new HttpClientSettingsKey(
            mode,
            proxyHost,
            proxyPort,
            nonProxyHosts,
            proxyUser,
            proxyPassword,
            proxyProtocol,
            userAgentSuffix,
            gzipDisabled);
      }
    }
    // if no proxy properties, return key with only OCSP mode
    return new HttpClientSettingsKey(mode);
  }

  /**
   * Round the time value from milliseconds to seconds so the seconds can be used to create
   * SimpleDateFormatter. Negative values have to be rounded to the next negative value, while
   * positive values should be cut off with no rounding.
   *
   * @param millis milliseconds
   * @return seconds as long value
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

  /**
   * Get the time value in session timezone instead of UTC calculation done by java.sql.Time.
   *
   * @param time time in seconds
   * @param nanos nanoseconds
   * @return time in session timezone
   */
  public static Time getTimeInSessionTimezone(Long time, int nanos) {
    LocalDateTime lcd = LocalDateTime.ofEpochSecond(time, nanos, ZoneOffset.UTC);
    Time ts = Time.valueOf(lcd.toLocalTime());
    // Time.valueOf() will create the time without the nanoseconds i.e. only hh:mm:ss
    // Using calendar to add the nanoseconds back to time
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(ts.getTime());
    c.add(Calendar.MILLISECOND, nanos / 1000000);
    ts.setTime(c.getTimeInMillis());
    return ts;
  }

  /**
   * Helper function to convert system properties to boolean
   *
   * @param systemProperty name of the system property
   * @param defaultValue default value used
   * @return the value of the system property as boolean, else the default value
   */
  @SnowflakeJdbcInternalApi
  public static boolean convertSystemPropertyToBooleanValue(
      String systemProperty, boolean defaultValue) {
    String systemPropertyValue = systemGetProperty(systemProperty);
    if (systemPropertyValue != null) {
      return Boolean.parseBoolean(systemPropertyValue);
    }
    return defaultValue;
  }
  /**
   * Helper function to convert environment variable to boolean
   *
   * @param envVariableKey property name of the environment variable
   * @param defaultValue default value used
   * @return the value of the environment variable as boolean, else the default value
   */
  @SnowflakeJdbcInternalApi
  public static boolean convertSystemGetEnvToBooleanValue(
      String envVariableKey, boolean defaultValue) {
    String environmentVariableValue = systemGetEnv(envVariableKey);
    if (environmentVariableValue != null) {
      return Boolean.parseBoolean(environmentVariableValue);
    }
    return defaultValue;
  }

  @SnowflakeJdbcInternalApi
  public static <T> T mapSFExceptionToSQLException(ThrowingCallable<T, SFException> action)
      throws SQLException {
    try {
      return action.call();
    } catch (SFException e) {
      throw new SQLException(e);
    }
  }

  public static String getJsonNodeStringValue(JsonNode node) throws SFException {
    if (node.isNull()) {
      return null;
    }
    return node.isValueNode() ? node.asText() : node.toString();
  }

  /**
   * Method introduced to avoid inconsistencies in custom headers handling, since these are defined
   * on drivers side e.g. some drivers might internally convert headers to canonical form.
   *
   * @param input map input
   * @return case insensitive map
   */
  @SnowflakeJdbcInternalApi
  public static Map<String, String> createCaseInsensitiveMap(Map<String, String> input) {
    Map<String, String> caseInsensitiveMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    if (input != null) {
      caseInsensitiveMap.putAll(input);
    }
    return caseInsensitiveMap;
  }

  /**
   * toCaseInsensitiveMap, but adjusted to Headers[] argument type
   *
   * @param headers array of headers
   * @return case insensitive map
   */
  @SnowflakeJdbcInternalApi
  public static Map<String, String> createCaseInsensitiveMap(Header[] headers) {
    if (headers != null) {
      return createCaseInsensitiveMap(
          stream(headers)
              .collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue)));
    } else {
      return new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }
  }

  /**
   * Check whether the OS is Windows
   *
   * @return boolean
   */
  @SnowflakeJdbcInternalApi
  public static boolean isWindows() {
    return Constants.getOS() == Constants.OS.WINDOWS;
  }

  public static boolean isNullOrEmpty(String str) {
    return str == null || str.isEmpty();
  }
}
