package com.demo.retrieval.service.sequence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Read-side unpacking of the packed column strings. Serving never writes sequences. */
public final class SequenceCodec {

    private SequenceCodec() {
    }

    /**
     * Split into exactly {@code n} elements: pad short columns, truncate long ones.
     * The {@code -1} limit is required — the default drops trailing empty strings, which
     * would shift every row whose last value is null.
     */
    public static List<String> unpack(String packed, int n) {
        if (n <= 0) {
            return List.of();
        }
        String[] parts = (packed == null || packed.isEmpty())
            ? new String[0]
            : packed.split(SequenceSchemaConstants.ROW_SEPARATOR, -1);

        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(i < parts.length ? parts[i] : "");
        }
        return List.copyOf(out);
    }

    /**
     * Authoritative row count for a chunk: the {@code n} field, clamped down to the shortest
     * present data column. A torn write must produce fewer rows, never mis-aligned ones.
     */
    public static int rowCount(Map<String, String> fields) {
        int declared = parseCount(fields.get(SequenceSchemaConstants.COL_COUNT));
        if (declared <= 0) {
            return 0;
        }
        int shortest = declared;
        for (String column : SequenceSchemaConstants.COLUMNS) {
            String packed = fields.get(column);
            if (packed == null) {
                continue;   // column absent entirely — nothing to clamp against
            }
            // NOTE: no empty-string special case. A packed "" encodes exactly ONE
            // empty element (a single null value); zero elements is expressed by
            // n == 0, never by the string. Treating "" as length 0 would silently
            // drop every single-event chunk that has a null field — which is the
            // normal shape for a click event with no rating.
            int length = packed.split(SequenceSchemaConstants.ROW_SEPARATOR, -1).length;
            shortest = Math.min(shortest, length);
        }
        return Math.max(0, shortest);
    }

    private static int parseCount(String raw) {
        if (raw == null) {
            return 0;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
