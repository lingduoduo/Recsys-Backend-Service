package com.demo.retrieval.service.sequence;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisSequenceClientTest {

    // 2026-07-23T12:00:00Z — "today" is bucket 20260723
    private static final long NOW = 1784808000000L;
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC);

    /** A fake Redis backed by an in-memory map. */
    private static final class RecordingRedis {
        final List<List<String>> fetchedKeyBatches = new ArrayList<>();
        final StringRedisTemplate template;

        @SuppressWarnings({"unchecked", "rawtypes"})
        RecordingRedis(Map<String, Map<String, String>> store) {
            this.template = mock(StringRedisTemplate.class);
            HashOperations hashOps = mock(HashOperations.class);
            when(template.opsForHash()).thenReturn(hashOps);

            // Results of the multiGet calls issued during the current pipeline, in order.
            List<Object> pipelineResults = new ArrayList<>();

            when(hashOps.multiGet(anyString(), any())).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                List<Object> fields = new ArrayList<>((java.util.Collection<Object>) invocation.getArgument(1));
                Map<String, String> chunk = store.getOrDefault(key, Map.of());
                List<Object> out = new ArrayList<>();
                for (Object field : fields) {
                    out.add(chunk.get(String.valueOf(field)));
                }
                pipelineResults.add(out);   // real executePipelined collects each command's result
                return out;
            });

            when(template.executePipelined(any(SessionCallback.class))).thenAnswer(invocation -> {
                SessionCallback callback = invocation.getArgument(0);
                pipelineResults.clear();
                callback.execute(template);   // triggers the multiGet stub for each key, in order
                return new ArrayList<>(pipelineResults);
            });
        }
    }

    private static Map<String, String> chunk(String itemIds, String ts, int n) {
        return Map.of(
            SequenceSchemaConstants.COL_ITEM_ID, itemIds,
            SequenceSchemaConstants.COL_TS, ts,
            SequenceSchemaConstants.COL_COUNT, String.valueOf(n)
        );
    }

    private static String key(String bucket) {
        return SequenceSchemaConstants.key("u1", SequenceSchemaConstants.KIND_RATING, bucket);
    }

    /** Client that records every batch of keys it fetches, so the walk's cost is assertable. */
    private static RedisSequenceClient recordingClient(RecordingRedis redis) {
        return recordingClient(redis, 7);
    }

    /** Same as above, with a caller-chosen {@code bucketFetchChunk} (e.g. to force multiple round trips). */
    private static RedisSequenceClient recordingClient(RecordingRedis redis, int chunk) {
        return new RedisSequenceClient(redis.template, chunk, CLOCK) {
            @Override
            protected List<Map<String, String>> fetchBatch(List<String> keys, List<String> fields) {
                redis.fetchedKeyBatches.add(List.copyOf(keys));
                return super.fetchBatch(keys, fields);
            }
        };
    }

    @Test
    void bucketsToWalkListsDaysNewestFirstInclusiveOfToday() {
        List<String> buckets = RedisSequenceClient.bucketsToWalk(NOW, Duration.ofDays(3));
        assertEquals(List.of("20260723", "20260722", "20260721"), buckets);
    }

    @Test
    void readReturnsRowsNewestFirstAcrossBucketBoundaries() {
        RecordingRedis redis = new RecordingRedis(Map.of(
            key("20260723"), chunk("m3,m4", "300,400", 2),
            key("20260722"), chunk("m1,m2", "100,200", 2)
        ));
        RedisSequenceClient client = new RedisSequenceClient(redis.template, 7, CLOCK);

        SequenceSlice slice = client.read("u1", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 10, Duration.ofDays(7));

        assertEquals(4, slice.size());
        assertEquals(List.of("m4", "m3", "m2", "m1"), slice.itemIds());
        assertEquals(List.of(400L, 300L, 200L, 100L), slice.timestamps());
    }

    @Test
    void readStopsAfterTheFirstChunkOnceMaxRowsIsFilled() {
        RecordingRedis redis = new RecordingRedis(Map.of(
            key("20260723"), chunk("m1,m2,m3", "100,200,300", 3)
        ));
        RedisSequenceClient client = recordingClient(redis);

        SequenceSlice slice = client.read("u1", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 2, Duration.ofDays(90));

        assertEquals(2, slice.size());
        assertEquals(List.of("m3", "m2"), slice.itemIds());
        // 90 days of lookback must not cost 90 buckets when the first chunk already suffices.
        assertEquals(1, redis.fetchedKeyBatches.size(), "expected a single pipelined round trip");
        assertEquals(7, redis.fetchedKeyBatches.get(0).size());
    }

    @Test
    void readBoundsTheWalkByLookback() {
        RecordingRedis redis = new RecordingRedis(Map.of(
            key("20260701"), chunk("old", "1", 1)   // 22 days ago, outside a 7-day lookback
        ));
        RedisSequenceClient client = recordingClient(redis);

        SequenceSlice slice = client.read("u1", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 10, Duration.ofDays(7));

        assertEquals(0, slice.size());
        List<String> walked = new ArrayList<>();
        redis.fetchedKeyBatches.forEach(walked::addAll);
        assertEquals(7, walked.size());
        assertTrue(walked.stream().noneMatch(k -> k.endsWith("20260701")));
    }

    @Test
    void readAlwaysFetchesTsAndCountEvenWhenNotRequested() {
        RecordingRedis redis = new RecordingRedis(Map.of(
            key("20260723"), chunk("m1", "100", 1)
        ));
        RedisSequenceClient client = new RedisSequenceClient(redis.template, 7, CLOCK);

        SequenceSlice slice = client.read("u1", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 10, Duration.ofDays(7));

        assertEquals(List.of(100L), slice.timestamps());
        assertEquals(List.of("m1"), slice.itemIds());
        // Column pruning is real: genres was never requested, so it is not populated.
        assertEquals(List.of(), slice.column(SequenceSchemaConstants.COL_GENRES));
    }

    @Test
    void readReturnsEmptyForAUserWithNoBuckets() {
        RecordingRedis redis = new RecordingRedis(Map.of());
        RedisSequenceClient client = new RedisSequenceClient(redis.template, 7, CLOCK);

        SequenceSlice slice = client.read("ghost", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 10, Duration.ofDays(7));

        assertEquals(0, slice.size());
        assertEquals(List.of(), slice.itemIds());
    }

    @Test
    void readClampsAChunkWhoseColumnsDisagreeWithN() {
        RecordingRedis redis = new RecordingRedis(Map.of(
            key("20260723"), Map.of(
                SequenceSchemaConstants.COL_ITEM_ID, "m1,m2",
                SequenceSchemaConstants.COL_TS, "100,200",
                SequenceSchemaConstants.COL_COUNT, "5"   // torn write
            )
        ));
        RedisSequenceClient client = new RedisSequenceClient(redis.template, 7, CLOCK);

        SequenceSlice slice = client.read("u1", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 10, Duration.ofDays(7));

        assertEquals(2, slice.size());
        assertEquals(List.of("m2", "m1"), slice.itemIds());
    }

    @Test
    void readSortsGloballyAcrossBucketsEvenWhenNewerRowsAreInAnOlderBucket() {
        // Newest timestamps live in yesterday's (later-fetched) bucket; today's bucket only
        // holds the oldest rows. A per-bucket-sort-then-concat implementation would emit
        // today's rows first regardless of ts and get this wrong.
        RecordingRedis redis = new RecordingRedis(Map.of(
            key("20260723"), chunk("m1,m2", "100,200", 2),
            key("20260722"), chunk("m3,m4", "300,400", 2)
        ));
        RedisSequenceClient client = new RedisSequenceClient(redis.template, 7, CLOCK);

        SequenceSlice slice = client.read("u1", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 10, Duration.ofDays(7));

        assertEquals(4, slice.size());
        assertEquals(List.of("m4", "m3", "m2", "m1"), slice.itemIds());
        assertEquals(List.of(400L, 300L, 200L, 100L), slice.timestamps());
    }

    @Test
    void readContinuesToASecondRoundTripWhenFirstBatchDoesNotFillMaxRows() {
        // bucketFetchChunk of 1 forces one bucket per round trip. Today has a row, the next
        // day back is empty, and two days back has the row that finally fills maxRows — so
        // the walk must survive past the first (and second) pipelined batch.
        RecordingRedis redis = new RecordingRedis(Map.of(
            key("20260723"), chunk("mA", "500", 1),
            key("20260721"), chunk("mB", "100", 1)
        ));
        RedisSequenceClient client = recordingClient(redis, 1);

        SequenceSlice slice = client.read("u1", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 2, Duration.ofDays(3));

        assertEquals(2, slice.size());
        assertEquals(List.of("mA", "mB"), slice.itemIds());
        assertEquals(List.of(500L, 100L), slice.timestamps());
        assertTrue(redis.fetchedKeyBatches.size() >= 2, "expected more than one pipelined round trip");
    }

    @Test
    void readWithAPartialDayLookbackWalksIntoYesterdaysBucket() {
        // NOTE: the task spec's example used Duration.ofHours(18), but with NOW fixed at
        // 12:00:00Z, Math.max(1, ceil(hours/24)) equals 1 for ANY sub-24h duration (floor and
        // ceil agree below one full day), so 18h cannot discriminate the fix — it fails
        // identically before and after. 30h is a partial (non-whole-day) lookback that still
        // reaches into yesterday (2026-07-22T06:00:00Z) but does differentiate: floor gives 1
        // day (misses yesterday), ceil gives 2 (walks it). See task-11-report.md for the math.
        RecordingRedis redis = new RecordingRedis(Map.of(
            key("20260722"), chunk("m1", "100", 1)
        ));
        RedisSequenceClient client = new RedisSequenceClient(redis.template, 7, CLOCK);

        SequenceSlice slice = client.read("u1", SequenceSchemaConstants.KIND_RATING,
            Set.of(SequenceSchemaConstants.COL_ITEM_ID), 10, Duration.ofHours(30));

        assertEquals(1, slice.size());
        assertEquals(List.of("m1"), slice.itemIds());

        List<String> buckets = RedisSequenceClient.bucketsToWalk(NOW, Duration.ofHours(30));
        assertTrue(buckets.contains("20260723"));
        assertTrue(buckets.contains("20260722"));
    }
}
