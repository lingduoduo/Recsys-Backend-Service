package com.demo.retrieval.service.sequence;

import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Walks day buckets newest-first, pipelining a fixed number per round trip and stopping as
 * soon as enough rows are held. A user active today costs one round trip regardless of how
 * long the lookback window is.
 */
public class RedisSequenceClient implements SequenceClient {

    private final StringRedisTemplate redis;
    private final int bucketFetchChunk;
    private final Clock clock;

    public RedisSequenceClient(StringRedisTemplate redis, int bucketFetchChunk, Clock clock) {
        this.redis = redis;
        this.bucketFetchChunk = Math.max(1, bucketFetchChunk);
        this.clock = clock;
    }

    /** Day stamps from today backwards, inclusive, bounded by {@code lookback}. */
    public static List<String> bucketsToWalk(long nowMillis, Duration lookback) {
        int days = Math.max(1, (int) Math.ceil(lookback.toMillis() / 86_400_000.0));
        List<String> buckets = new ArrayList<>(days);
        for (int i = 0; i < days; i++) {
            buckets.add(SequenceSchemaConstants.bucket(nowMillis - (long) i * 86_400_000L));
        }
        return List.copyOf(buckets);
    }

    @Override
    public SequenceSlice read(String userId, String kind, Set<String> columns, int maxRows, Duration lookback) {
        if (maxRows <= 0) {
            return SequenceSlice.empty();
        }

        // ts and n are always fetched: ordering across buckets needs ts, and n is the
        // alignment authority. Everything else is exactly what the caller asked for.
        List<String> fields = new ArrayList<>();
        fields.add(SequenceSchemaConstants.COL_TS);
        fields.add(SequenceSchemaConstants.COL_COUNT);
        for (String column : SequenceSchemaConstants.COLUMNS) {
            if (columns.contains(column) && !fields.contains(column)) {
                fields.add(column);
            }
        }

        List<String> buckets = bucketsToWalk(clock.millis(), lookback);
        List<Map<String, List<String>>> decoded = new ArrayList<>();
        int held = 0;

        for (int offset = 0; offset < buckets.size() && held < maxRows; offset += bucketFetchChunk) {
            List<String> batch = new ArrayList<>();
            for (int i = offset; i < Math.min(offset + bucketFetchChunk, buckets.size()); i++) {
                batch.add(SequenceSchemaConstants.key(userId, kind, buckets.get(i)));
            }

            for (Map<String, String> chunk : fetchBatch(batch, fields)) {
                int n = SequenceCodec.rowCount(chunk);
                if (n == 0) {
                    continue;
                }
                Map<String, List<String>> unpacked = new LinkedHashMap<>();
                for (String field : fields) {
                    if (!SequenceSchemaConstants.COL_COUNT.equals(field)) {
                        unpacked.put(field, SequenceCodec.unpack(chunk.get(field), n));
                    }
                }
                decoded.add(unpacked);
                held += n;
            }
        }

        return assemble(decoded, fields, columns, maxRows);
    }

    /** One pipelined round trip. Overridable so tests can assert on the walk's cost. */
    protected List<Map<String, String>> fetchBatch(List<String> keys, List<String> fields) {
        List<Object> fieldKeys = new ArrayList<>(fields);
        List<Object> raw = redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> ops) {
                RedisOperations<String, String> typed = (RedisOperations<String, String>) ops;
                for (String key : keys) {
                    typed.opsForHash().multiGet(key, fieldKeys);
                }
                return null;
            }
        });

        List<Map<String, String>> out = new ArrayList<>(keys.size());
        for (Object entry : raw) {
            Map<String, String> chunk = new HashMap<>();
            if (entry instanceof List<?> values) {
                for (int i = 0; i < fields.size() && i < values.size(); i++) {
                    Object value = values.get(i);
                    if (value != null) {
                        chunk.put(fields.get(i), String.valueOf(value));
                    }
                }
            }
            out.add(chunk);
        }
        return out;
    }

    /** Flatten decoded chunks into one ts-descending slice, keeping the layout columnar. */
    private SequenceSlice assemble(
        List<Map<String, List<String>>> decoded,
        List<String> fields,
        Set<String> requested,
        int maxRows
    ) {
        record Ref(long ts, int chunk, int row) {
        }

        List<Ref> order = new ArrayList<>();
        for (int c = 0; c < decoded.size(); c++) {
            List<String> timestamps = decoded.get(c).getOrDefault(SequenceSchemaConstants.COL_TS, List.of());
            for (int r = 0; r < timestamps.size(); r++) {
                order.add(new Ref(parseLong(timestamps.get(r)), c, r));
            }
        }
        order.sort(Comparator.comparingLong(Ref::ts).reversed());
        if (order.size() > maxRows) {
            order = order.subList(0, maxRows);
        }

        List<String> emitted = new ArrayList<>(fields);
        emitted.remove(SequenceSchemaConstants.COL_COUNT);
        emitted.removeIf(f -> !requested.contains(f) && !SequenceSchemaConstants.COL_TS.equals(f));

        Map<String, List<String>> out = new LinkedHashMap<>();
        for (String field : emitted) {
            List<String> values = new ArrayList<>(order.size());
            for (Ref ref : order) {
                List<String> column = decoded.get(ref.chunk()).getOrDefault(field, List.of());
                values.add(ref.row() < column.size() ? column.get(ref.row()) : "");
            }
            out.put(field, List.copyOf(values));
        }
        return new SequenceSlice(out, order.size());
    }

    private static long parseLong(String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException | NullPointerException e) {
            return 0L;
        }
    }
}
