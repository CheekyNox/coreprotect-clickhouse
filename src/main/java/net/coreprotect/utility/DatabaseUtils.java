package net.coreprotect.utility;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
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
        if (ConfigHandler.databaseType.isClickHouse() && isClickHouseBinaryArray(resultSet, columnIndex)) {
            return decodeClickHouseBinary(clickHouseBinaryValue(resultSet, columnIndex), column);
        }

        byte[] value;
        try {
            value = resultSet.getBytes(column);
        }
        catch (SQLFeatureNotSupportedException e) {
            try {
                value = resultSet.getBytes(columnIndex);
            }
            catch (SQLException inner) {
                if (ConfigHandler.databaseType.isClickHouse() && isClickHouseBinaryReadException(inner)) {
                    return decodeClickHouseBinary(clickHouseBinaryValue(resultSet, columnIndex), column);
                }
                throw inner;
            }
        }
        catch (SQLException e) {
            if (ConfigHandler.databaseType.isClickHouse() && isClickHouseBinaryReadException(e)) {
                return decodeClickHouseBinary(clickHouseBinaryValue(resultSet, columnIndex), column);
            }
            throw e;
        }
        return value;
    }

    private static boolean isClickHouseBinaryArray(ResultSet resultSet, int columnIndex) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        if (metadata.getColumnType(columnIndex) == Types.ARRAY) {
            return true;
        }

        String typeName = metadata.getColumnTypeName(columnIndex);
        return isClickHouseArrayTypeName(typeName);
    }

    static boolean isClickHouseArrayTypeName(String typeName) {
        if (typeName == null) {
            return false;
        }
        String normalized = typeName.replace(" ", "");
        return normalized.startsWith("Array(") || normalized.startsWith("Nullable(Array(");
    }

    private static boolean isClickHouseBinaryReadException(SQLException exception) {
        String message = exception.getMessage();
        return message != null && message.contains("Column is not of array type");
    }

    private static Object clickHouseBinaryValue(ResultSet resultSet, int columnIndex) throws SQLException {
        SQLException failure = null;

        try {
            return resultSet.getObject(columnIndex);
        }
        catch (SQLException e) {
            failure = e;
        }

        try {
            return resultSet.getObject(columnIndex, Object.class);
        }
        catch (SQLException e) {
            failure.addSuppressed(e);
        }

        try {
            java.sql.Array array = resultSet.getArray(columnIndex);
            return array == null ? null : array.getArray();
        }
        catch (SQLException e) {
            failure.addSuppressed(e);
        }

        try {
            return parseClickHouseArrayString(resultSet.getString(columnIndex));
        }
        catch (SQLException e) {
            failure.addSuppressed(e);
        }

        throw failure;
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
        else if (rawValue instanceof String) {
            return decodeClickHouseBinary(parseClickHouseArrayString((String) rawValue), column);
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

    private static List<Integer> parseClickHouseArrayString(String value) throws SQLException {
        if (value == null) {
            return null;
        }

        String trimmed = value.trim();
        if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) {
            throw new SQLException("Invalid ClickHouse binary array string: " + value);
        }

        String body = trimmed.substring(1, trimmed.length() - 1).trim();
        List<Integer> result = new ArrayList<>();
        if (body.isEmpty()) {
            return result;
        }

        String[] parts = body.split(",");
        for (String part : parts) {
            try {
                result.add(Integer.parseInt(part.trim()));
            }
            catch (NumberFormatException e) {
                throw new SQLException("Invalid ClickHouse binary array element: " + part, e);
            }
        }
        return result;
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
