package com.demo.retrieval.service.sequence;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Mirror of the Scala {@code com.demo.sequence.SequenceSchema}. Cross-language drift is the
 * most exposed failure mode of the sequence store, so both sides assert against the shared
 * {@code sequence-schema.json} test fixture.
 */
public final class SequenceSchemaConstants {

    public static final String KEY_PREFIX = "seq";

    public static final String KIND_RATING = "rating";
    public static final String KIND_CLICK = "click";
    public static final String KIND_BEHAVIOR = "behavior";

    public static final String COL_ITEM_ID = "item_id";
    public static final String COL_TS = "ts";
    public static final String COL_ACTION = "action";
    public static final String COL_RATING = "rating";
    public static final String COL_GENRES = "genres";
    public static final String COL_RELEASE_YEAR = "release_year";
    public static final String COL_COUNT = "n";

    public static final List<String> COLUMNS =
        List.of(COL_ITEM_ID, COL_TS, COL_ACTION, COL_RATING, COL_GENRES, COL_RELEASE_YEAR);

    public static final String ROW_SEPARATOR = ",";
    public static final String VALUE_SEPARATOR = "|";

    private static final DateTimeFormatter DAY_FORMAT =
        DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private SequenceSchemaConstants() {
    }

    public static String key(String userId, String kind, String bucket) {
        return KEY_PREFIX + ":" + userId + ":" + kind + ":" + bucket;
    }

    public static String bucket(long epochMillis) {
        return DAY_FORMAT.format(Instant.ofEpochMilli(epochMillis));
    }
}
