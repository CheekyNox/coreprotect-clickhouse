package net.coreprotect.utility;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

class DatabaseUtilsTest {

    @Test
    void decodesClickHouseBinaryList() throws Exception {
        assertArrayEquals(new byte[] { (byte) 0xAC, (byte) 0xED },
                DatabaseUtils.decodeClickHouseBinary(Arrays.asList((byte) 0, (byte) 0xAC, (byte) 0xED), "metadata"));
    }

    @Test
    void decodesClickHousePrimitiveArray() throws Exception {
        assertArrayEquals(new byte[] { 1, -1 },
                DatabaseUtils.decodeClickHouseBinary(new byte[] { 0, 1, -1 }, "metadata"));
    }

    @Test
    void decodesClickHouseArrayStringFallback() throws Exception {
        assertArrayEquals(new byte[] { (byte) 0xAC, (byte) 0xED },
                DatabaseUtils.decodeClickHouseBinary("[0, -84, -19]", "metadata"));
    }

    @Test
    void detectsClickHouseArrayTypeNames() {
        assertTrue(DatabaseUtils.isClickHouseArrayTypeName("Array(Int8)"));
        assertTrue(DatabaseUtils.isClickHouseArrayTypeName("Nullable(Array(Int8))"));
        assertFalse(DatabaseUtils.isClickHouseArrayTypeName("String"));
    }

    @Test
    void treatsNullAndEmptyArraysAsNull() throws Exception {
        assertNull(DatabaseUtils.decodeClickHouseBinary(null, "metadata"));
        assertNull(DatabaseUtils.decodeClickHouseBinary(Collections.emptyList(), "metadata"));
    }

    @Test
    void rejectsInvalidPresenceMarker() {
        assertThrows(SQLException.class, () -> DatabaseUtils.decodeClickHouseBinary(new byte[] { 1, 2 }, "metadata"));
    }

    @Test
    void rejectsValuesOutsideInt8Range() {
        assertThrows(SQLException.class, () -> DatabaseUtils.decodeClickHouseBinary(Arrays.asList(0, 128), "metadata"));
    }
}
