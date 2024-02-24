package net.snowflake.client.core;

import net.snowflake.client.core.json.Converters;
import net.snowflake.client.core.structs.SQLDataCreationHelper;
import net.snowflake.client.jdbc.FieldMetadata;
import net.snowflake.client.jdbc.SnowflakeLoggedFeatureNotSupportedException;
import net.snowflake.client.util.ThrowingBiFunction;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import org.apache.arrow.vector.util.JsonStringHashMap;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import static net.snowflake.client.jdbc.SnowflakeUtil.mapExceptions;

@SnowflakeJdbcInternalApi
public class ArrowSqlInput implements SFSqlInput {

    private final JsonStringHashMap<String, Object> input;
    private final SFBaseSession session;
    private final Iterator<Object> elements;
    private final Converters converters;
    private final List<FieldMetadata> fields;

    private int currentIndex = 0;

    public ArrowSqlInput(JsonStringHashMap<String, Object> input, SFBaseSession session, Converters converters, List<FieldMetadata> fields) {
        this.input = input;
        this.elements = input.values().iterator();
        this.session = session;
        this.converters = converters;
        this.fields = fields;
    }

    @Override
    public String readString() throws SQLException {
        return withNextValue(
                ((value, fieldMetadata) -> {
                    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
                    int columnSubType = fieldMetadata.getType();
                    int scale = fieldMetadata.getScale();
                    return mapExceptions(
                            () ->
                                    converters
                                            .getStringConverter()
                                            .getString(value, columnType, columnSubType, scale));
                }));
    }

    @Override
    public boolean readBoolean() throws SQLException {
        return withNextValue((value, fieldMetadata) -> {
            int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
            return mapExceptions(
                    () -> converters.getBooleanConverter().getBoolean(value, columnType));
        });
    }

    @Override
    public byte readByte() throws SQLException {
        return withNextValue(
                (value, fieldMetadata) ->
                        mapExceptions(() -> converters.getNumberConverter().getByte(value)));
    }

    @Override
    public short readShort() throws SQLException {
        return withNextValue(
                (value, fieldMetadata) -> {
                    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
                    return mapExceptions(() -> converters.getNumberConverter().getShort(value, columnType));
                });
    }

    @Override
    public int readInt() throws SQLException {
        return withNextValue(
                (value, fieldMetadata) -> {
                    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
                    return mapExceptions(() -> converters.getNumberConverter().getInt(value, columnType));
                });
    }

    @Override
    public long readLong() throws SQLException {
        return withNextValue(
                (value, fieldMetadata) -> {
                    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
                    return mapExceptions(() -> converters.getNumberConverter().getLong(value, columnType));
                });
    }

    @Override
    public float readFloat() throws SQLException {
        return withNextValue(
                (value, fieldMetadata) -> {
                    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
                    return mapExceptions(() -> converters.getNumberConverter().getFloat(value, columnType));
                });
    }

    @Override
    public double readDouble() throws SQLException {
        return withNextValue(
                (value, fieldMetadata) -> {
                    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
                    return mapExceptions(() -> converters.getNumberConverter().getDouble(value, columnType));
                });
    }

    @Override
    public BigDecimal readBigDecimal() throws SQLException {
        return withNextValue(
                (value, fieldMetadata) -> {
                    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
                    return mapExceptions(
                            () -> converters.getNumberConverter().getBigDecimal(value, columnType));
                });
    }

    @Override
    public byte[] readBytes() throws SQLException {
        return withNextValue(
                (value, fieldMetadata) -> {
                    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
                    int columnSubType = fieldMetadata.getType();
                    int scale = fieldMetadata.getScale();
                    return mapExceptions(
                            () ->
                                    converters.getBytesConverter().getBytes(value, columnType, columnSubType, scale));
                });
    }

    @Override
    public Date readDate() throws SQLException {
        return withNextValue(
                (value, fieldMetadata) -> {
                    SnowflakeDateTimeFormat formatter = SqlInputTimestampUtil.extractDateTimeFormat(session, "DATE_OUTPUT_FORMAT");
                    SFTimestamp timestamp = formatter.parse((String) value);
                    return Date.valueOf(
                            Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalDate());
                });
    }

    @Override
    public Time readTime() throws SQLException {
        return withNextValue(
                (value, fieldMetadata) -> {
                    SnowflakeDateTimeFormat formatter = SqlInputTimestampUtil.extractDateTimeFormat(session, "TIME_OUTPUT_FORMAT");
                    SFTimestamp timestamp = formatter.parse((String) value);
                    return Time.valueOf(
                            Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalTime());
                });
    }

    @Override
    public Timestamp readTimestamp() throws SQLException {
        return readTimestamp(null);
    }

    @Override
    public Timestamp readTimestamp(TimeZone tz) throws SQLException {
        return withNextValue(
                (value, fieldMetadata) -> {
                    if (value == null) {
                        return null;
                    }
                    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
                    int columnSubType = fieldMetadata.getType();
                    int scale = fieldMetadata.getScale();
                    // TODO structuredType what if not a string value?
                    Timestamp result = SqlInputTimestampUtil.getTimestampFromType(columnSubType, (String) value, session);
                    if (result != null) {
                        return result;
                    }
                    return mapExceptions(
                            () ->
                                    converters
                                            .getDateTimeConverter()
                                            .getTimestamp(value, columnType, columnSubType, tz, scale));
                });
    }


    @Override
    public Reader readCharacterStream() throws SQLException {
        throw new SnowflakeLoggedFeatureNotSupportedException(session, "readCharacterStream");
    }

    @Override
    public InputStream readAsciiStream() throws SQLException {
        throw new SnowflakeLoggedFeatureNotSupportedException(session, "readAsciiStream");
    }

    @Override
    public InputStream readBinaryStream() throws SQLException {
        throw new SnowflakeLoggedFeatureNotSupportedException(session, "readBinaryStream");
    }

    @Override
    public Object readObject() throws SQLException {
        return withNextValue((value, fieldMetadata) -> {
            if (!(value instanceof JsonStringHashMap)) {
                throw new SQLException("Invalid value passed to 'readObject()', expected Map; got: " + value.getClass());
            }
            return value;
        });
    }

    @Override
    public <T> T readObject(Class<T> type) throws SQLException {
        return withNextValue(
                (value, fieldMetadata) -> {
                    SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
                    instance.readSQL(
                            new ArrowSqlInput(
                                    (JsonStringHashMap<String, Object>) value,
                                    session,
                                    converters,
                                    Arrays.asList(fieldMetadata.getFields())
                            ),
                            null
                    );
                    return (T) instance;
                });
    }

    @Override
    public Ref readRef() throws SQLException {
        throw new SnowflakeLoggedFeatureNotSupportedException(session, "readRef");
    }

    @Override
    public Blob readBlob() throws SQLException {
        throw new SnowflakeLoggedFeatureNotSupportedException(session, "readBlob");
    }

    @Override
    public Clob readClob() throws SQLException {
        throw new SnowflakeLoggedFeatureNotSupportedException(session, "readClob");
    }

    @Override
    public Array readArray() throws SQLException {
        throw new SnowflakeLoggedFeatureNotSupportedException(session, "readArray");
    }

    @Override
    public boolean wasNull() throws SQLException {
        return false; // nulls are not allowed in structure types
    }

    @Override
    public URL readURL() throws SQLException {
        throw new SnowflakeLoggedFeatureNotSupportedException(session, "readCharacterStream");
    }

    @Override
    public NClob readNClob() throws SQLException {
        throw new SnowflakeLoggedFeatureNotSupportedException(session, "readNClob");
    }

    @Override
    public String readNString() throws SQLException {
        throw new SnowflakeLoggedFeatureNotSupportedException(session, "readNString");
    }

    @Override
    public SQLXML readSQLXML() throws SQLException {
        throw new SnowflakeLoggedFeatureNotSupportedException(session, "readSQLXML");
    }

    @Override
    public RowId readRowId() throws SQLException {
        throw new SnowflakeLoggedFeatureNotSupportedException(session, "readRowId");
    }

    private <T> T withNextValue(
            ThrowingBiFunction<Object, FieldMetadata, T, SQLException> action)
            throws SQLException {
        return action.apply(elements.next(), fields.get(currentIndex++));
    }
}
