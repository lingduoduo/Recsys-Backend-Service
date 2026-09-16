package com.demo.retrieval.service.sequence;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequenceCodecTest {

    @Test
    void unpackRoundTripsASimpleColumn() {
        assertEquals(List.of("31", "1029", "1061"), SequenceCodec.unpack("31,1029,1061", 3));
    }

    @Test
    void unpackPreservesTrailingNullsAsEmptyElements() {
        // String.split(",") without limit -1 returns ["", "4.0"] and shifts every later row.
        assertEquals(List.of("", "4.0", ""), SequenceCodec.unpack(",4.0,", 3));
    }

    @Test
    void unpackTreatsEmptyStringAsZeroRows() {
        assertEquals(List.of(), SequenceCodec.unpack("", 0));
    }

    @Test
    void unpackPadsAShortColumnInsteadOfMisaligning() {
        assertEquals(List.of("a", "b", "", ""), SequenceCodec.unpack("a,b", 4));
    }

    @Test
    void unpackTruncatesALongColumn() {
        assertEquals(List.of("a", "b"), SequenceCodec.unpack("a,b,c,d", 2));
    }

    @Test
    void rowCountUsesTheCountField() {
        assertEquals(3, SequenceCodec.rowCount(Map.of(
            SequenceSchemaConstants.COL_COUNT, "3",
            SequenceSchemaConstants.COL_ITEM_ID, "a,b,c"
        )));
    }

    @Test
    void rowCountClampsToTheShortestColumnWhenTheyDisagree() {
        // A torn write must degrade to fewer rows, never mis-align them.
        assertEquals(2, SequenceCodec.rowCount(Map.of(
            SequenceSchemaConstants.COL_COUNT, "3",
            SequenceSchemaConstants.COL_ITEM_ID, "a,b",
            SequenceSchemaConstants.COL_TS, "1,2"
        )));
    }

    @Test
    void rowCountDoesNotDropASingleRowWhoseValueIsNull() {
        // A packed "" is ONE empty element, not zero. Treating it as zero would drop
        // every single-event chunk with a null field — the normal shape for a click.
        assertEquals(1, SequenceCodec.rowCount(Map.of(
            SequenceSchemaConstants.COL_COUNT, "1",
            SequenceSchemaConstants.COL_ITEM_ID, "m1",
            SequenceSchemaConstants.COL_TS, "1000",
            SequenceSchemaConstants.COL_RATING, ""
        )));
    }

    @Test
    void rowCountIsZeroForMissingOrUnparseableCount() {
        assertEquals(0, SequenceCodec.rowCount(Map.of()));
        assertEquals(0, SequenceCodec.rowCount(Map.of(SequenceSchemaConstants.COL_COUNT, "oops")));
    }

    @Test
    void constantsMatchTheSharedCrossLanguageFixture() throws Exception {
        String fixture = new String(
            getClass().getResourceAsStream("/sequence-schema.json").readAllBytes(),
            StandardCharsets.UTF_8
        );
        for (String column : SequenceSchemaConstants.COLUMNS) {
            assertTrue(fixture.contains("\"" + column + "\""), "missing column " + column);
        }
        assertTrue(fixture.contains("\"rowSeparator\": \"" + SequenceSchemaConstants.ROW_SEPARATOR + "\""));
        assertTrue(fixture.contains("\"valueSeparator\": \"" + SequenceSchemaConstants.VALUE_SEPARATOR + "\""));
        assertTrue(fixture.contains("\"keyPrefix\": \"" + SequenceSchemaConstants.KEY_PREFIX + "\""));
        assertTrue(fixture.contains("\"countField\": \"" + SequenceSchemaConstants.COL_COUNT + "\""));
        assertTrue(fixture.contains(
            "\"kinds\": [\"" + SequenceSchemaConstants.KIND_RATING + "\", \"" + SequenceSchemaConstants.KIND_CLICK + "\", \"" + SequenceSchemaConstants.KIND_BEHAVIOR + "\"]"));
    }

    @Test
    void keyMatchesTheScalaFormat() {
        assertEquals("seq:u1:rating:20260723",
            SequenceSchemaConstants.key("u1", SequenceSchemaConstants.KIND_RATING, "20260723"));
    }

    @Test
    void bucketMapsAWholeUtcDayToOneStamp() {
        assertEquals("20260723", SequenceSchemaConstants.bucket(1784764800000L));
        assertEquals("20260723", SequenceSchemaConstants.bucket(1784851199999L));
        assertEquals("20260724", SequenceSchemaConstants.bucket(1784851200000L));
    }

    @Test
    void sliceExposesRequestedColumnsAndEmptyListsForTheRest() {
        SequenceSlice slice = new SequenceSlice(
            Map.of(
                SequenceSchemaConstants.COL_ITEM_ID, List.of("m1", "m2"),
                SequenceSchemaConstants.COL_TS, List.of("100", "200")
            ),
            2
        );

        assertEquals(2, slice.size());
        assertEquals(List.of("m1", "m2"), slice.itemIds());
        assertEquals(List.of(100L, 200L), slice.timestamps());
        assertEquals(List.of(), slice.column(SequenceSchemaConstants.COL_GENRES));
    }
}
