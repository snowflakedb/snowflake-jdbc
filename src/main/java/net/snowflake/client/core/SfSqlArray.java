package net.snowflake.client.core;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class SfSqlArray<T> implements Array {

    private int base;
    private Object elements;
    private ResultSet resultSet;
    private int columnIndex;

    public SfSqlArray(int base, Object elements) {
        this.base = base;
        this.elements = elements;
    }

    @Override
        public String getBaseTypeName() throws SQLException {
            return null;
        }

        @Override
        public int getBaseType() throws SQLException {
            return 0;
        }

        @Override
        public Object getArray() throws SQLException {
            return elements;
        }

        @Override
        public Object getArray(Map<String, Class<?>> map) throws SQLException {
            return elements; //todo use method parameters
        }

        @Override
        public Object getArray(long index, int count) throws SQLException {
            return elements; //todo use method parameters
        }

        @Override
        public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
            return elements; //todo use method parameters
        }

        @Override
        public ResultSet getResultSet() throws SQLException {
            return null;
        }

        @Override
        public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
            return null;
        }

        @Override
        public ResultSet getResultSet(long index, int count) throws SQLException {
            return null;
        }

        @Override
        public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
                throws SQLException {
            return null;
        }

        @Override
        public void free() throws SQLException {}
    }
