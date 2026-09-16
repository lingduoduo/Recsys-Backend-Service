package com.demo.retrieval.service.sequence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A columnar read result. Deliberately not a {@code List<SequenceRow>}: decoding into row
 * objects at this boundary would reduce column pruning to a network-bytes optimisation
 * instead of a work saving.
 */
public final class SequenceSlice {

    private final Map<String, List<String>> columns;
    private final int size;

    public SequenceSlice(Map<String, List<String>> columns, int size) {
        this.columns = Map.copyOf(columns);
        this.size = size;
    }

    public static SequenceSlice empty() {
        return new SequenceSlice(Map.of(), 0);
    }

    public int size() {
        return size;
    }

    /** Empty list when the column was not requested — callers never need a null check. */
    public List<String> column(String name) {
        return columns.getOrDefault(name, List.of());
    }

    public List<String> itemIds() {
        return column(SequenceSchemaConstants.COL_ITEM_ID);
    }

    public List<Long> timestamps() {
        List<String> raw = column(SequenceSchemaConstants.COL_TS);
        List<Long> out = new ArrayList<>(raw.size());
        for (String value : raw) {
            out.add(parseLong(value));
        }
        return List.copyOf(out);
    }

    private static long parseLong(String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException | NullPointerException e) {
            return 0L;
        }
    }
}
