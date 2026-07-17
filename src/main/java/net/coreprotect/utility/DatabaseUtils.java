package net.coreprotect.utility;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import net.coreprotect.config.ConfigHandler;

public class DatabaseUtils {

    private DatabaseUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static <K, V extends Comparable<? super V>> SortedSet<Map.Entry<K, V>> entriesSortedByValues(Map<K, V> map) {
        SortedSet<Map.Entry<K, V>> sortedEntries = new TreeSet<>((e1, e2) -> {
            int res = e1.getValue().compareTo(e2.getValue());
            return res != 0 ? res : 1;
        });
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }

    public static byte[] getBytes(ResultSet resultSet, String column) throws SQLException {
        int columnIndex = resultSet.findColumn(column);
        if (ConfigHandler.databaseType.isClickHouse() && resultSet.getMetaData().getColumnType(columnIndex) == Types.ARRAY) {
            Object value;
            try {
                value = resultSet.getObject(columnIndex, Object.class);
            }
            catch (SQLFeatureNotSupportedException e) {
                java.sql.Array array = resultSet.getArray(columnIndex);
                value = array == null ? null : array.getArray();
            }
            return decodeClickHouseBinary(value, column);
        }

        byte[] value;
        try {
            value = resultSet.getBytes(column);
        }
        catch (SQLFeatureNotSupportedException e) {
            value = resultSet.getBytes(columnIndex);
        }
        return value;
    }

    static byte[] decodeClickHouseBinary(Object rawValue, String column) throws SQLException {
        if (rawValue == null) {
            return null;
        }
        if (rawValue instanceof java.sql.Array) {
            java.sql.Array array = (java.sql.Array) rawValue;
            try {
                return decodeClickHouseBinary(array.getArray(), column);
            }
            finally {
                array.free();
            }
        }

        byte[] value;
        if (rawValue instanceof byte[]) {
            value = (byte[]) rawValue;
        }
        else if (rawValue instanceof List<?>) {
            List<?> values = (List<?>) rawValue;
            value = new byte[values.size()];
            for (int i = 0; i < values.size(); i++) {
                value[i] = clickHouseByte(values.get(i), column);
            }
        }
        else if (rawValue.getClass().isArray()) {
            int length = java.lang.reflect.Array.getLength(rawValue);
            value = new byte[length];
            for (int i = 0; i < length; i++) {
                value[i] = clickHouseByte(java.lang.reflect.Array.get(rawValue, i), column);
            }
        }
        else {
            throw new SQLException("Unsupported ClickHouse binary value for column " + column + ": " + rawValue.getClass().getName());
        }

        if (value.length == 0) {
            return null;
        }
        if (value[0] != 0) {
            throw new SQLException("Invalid ClickHouse binary presence marker for column " + column);
        }
        byte[] binary = new byte[value.length - 1];
        System.arraycopy(value, 1, binary, 0, binary.length);
        return binary;
    }

    private static byte clickHouseByte(Object value, String column) throws SQLException {
        if (!(value instanceof Number)) {
            throw new SQLException("Invalid ClickHouse binary element for column " + column + ": " + String.valueOf(value));
        }
        int number = ((Number) value).intValue();
        if (number < Byte.MIN_VALUE || number > Byte.MAX_VALUE) {
            throw new SQLException("ClickHouse binary element out of Int8 range for column " + column + ": " + number);
        }
        return (byte) number;
    }

    public static String caseInsensitiveEquals(String column) {
        if (ConfigHandler.databaseType.isClickHouse()) {
            return "lowerUTF8(" + column + ") = lowerUTF8(?)";
        }
        return column + " = ?" + (ConfigHandler.databaseType.isMySQL() ? "" : " COLLATE NOCASE");
    }

    public static boolean successfulQuery(Connection connection, String query) {
        boolean result = false;
        try {
            PreparedStatement preparedStmt = connection.prepareStatement(query);
            ResultSet resultSet = preparedStmt.executeQuery();
            if (resultSet.isBeforeFirst()) {
                result = true;
            }
            resultSet.close();
            preparedStmt.close();
        }
        catch (Exception e) {
            ErrorReporter.report(e);
        }
        return result;
    }

}
