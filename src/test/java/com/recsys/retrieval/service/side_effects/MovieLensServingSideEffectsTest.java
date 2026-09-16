package com.demo.retrieval.service.side_effects;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.event.RecsysEventAvroCodec;
import com.demo.retrieval.model.FeatureCache;
import com.demo.retrieval.service.grpo.GrpoEventPublisher;
import com.demo.retrieval.service.grpo.GrpoPolicyScorer;
import com.demo.retrieval.service.replay.ReplayEvent;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class MovieLensServingSideEffectsTest {
    private StringRedisTemplate redis;
    private ValueOperations<String, String> valueOps;
    private SetOperations<String, String> setOps;
    private HashOperations<String, Object, Object> hashOps;
    private MovieLensServingSideEffects sideEffects;

    /** The default rollout state: the scorer must be inert, so these tests exercise nothing of it. */
    private static GrpoPolicyScorer offScorer() {
        RecommendationProperties properties = new RecommendationProperties();
        return new GrpoPolicyScorer(mock(StringRedisTemplate.class), properties, new FeatureCache(properties));
    }

    @BeforeEach
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        valueOps = mock(ValueOperations.class);
        setOps = mock(SetOperations.class);
        hashOps = mock(HashOperations.class);
        when(redis.opsForValue()).thenReturn(valueOps);
        when(redis.opsForSet()).thenReturn(setOps);
        when(redis.opsForHash()).thenReturn(hashOps);
        when(redis.executePipelined(any(SessionCallback.class))).thenAnswer(invocation -> {
            SessionCallback cb = invocation.getArgument(0);
            cb.execute(redis);
            return List.of();
        });
        sideEffects = new MovieLensServingSideEffects(redis, new ObjectMapper(), Duration.ofHours(1),
            new GrpoEventPublisher(new RecsysEventAvroCodec(), null, false),
            offScorer());
    }

    @Test
    void recordServedWritesMovieLensServingSideEffectsInOnePipeline() {
        ServedMovie top = servedMovie("m1", true);
        ServingSideEffectRequest request = new ServingSideEffectRequest(
            "req-1",
            "u1",
            "ucb",
            Map.of("recent", List.of("m0"), "genres", List.of("sci-fi"), "tags", List.of()),
            List.of(top, servedMovie("m2", false)),
            List.of(top),
            List.of("m3", "m1"),
            List.of("m4"),
            1,
            1,
            1,
            0.8,
            0.1,
            0.5
        );

        sideEffects.recordServed(request);

        verify(valueOps).increment("bandit:item:m1:impressions", 1);
        verify(setOps).add(MovieLensServingSideEffects.EXPOSED_ITEMS_KEY, "m1");
        verify(hashOps).put("user:u1:served_history", "movieIds", "m1,m3");
        verify(hashOps).put("user:u1:impressions", "movieIds", "m1,m4");
        verify(hashOps).putAll(eq("recommendation:request:req-1"), any(Map.class));
        verify(hashOps).increment(MovieLensServingSideEffects.METRICS_HASH_KEY, "requests", 1L);
        verify(hashOps).increment(MovieLensServingSideEffects.metricsHashKey("ucb"), "recommendations_served", 1L);
        ArgumentCaptor<String> pendingPayload = ArgumentCaptor.forClass(String.class);
        verify(valueOps).set(eq(MovieLensServingSideEffects.pendingReplayKey("u1", "m1")), pendingPayload.capture(), eq(Duration.ofHours(1)));
        String payload = pendingPayload.getValue();
        assertTrue(payload.contains("\"action\":\"m1\""));
        assertTrue(payload.contains("\"state\""));
        assertFalse(payload.contains("\"context\""), "context is a removed duplicate of state");
    }

    @Test
    void recordServedSkipsEmptySelections() {
        sideEffects.recordServed(new ServingSideEffectRequest(
            "req-empty",
            "u1",
            "ucb",
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            0,
            0,
            0,
            0.0,
            0.0,
            0.0
        ));

        verify(redis, org.mockito.Mockito.never()).executePipelined(any(SessionCallback.class));
    }

    @Test
    void replayContextAndEmittedImpressionShareTheServingRequestId() throws Exception {
        // The offline DPO arm joins the slate log (built from these Kafka events) to the replay
        // buffer on request id. Both sinks must carry the SAME value from one served request. Two
        // independent generators is exactly the defect that left that join matching nothing until
        // serving emitted its own events, so the identity is pinned here, where both writes happen.
        List<byte[]> sent = new ArrayList<>();
        RecsysEventAvroCodec codec = new RecsysEventAvroCodec();
        sideEffects = new MovieLensServingSideEffects(redis, new ObjectMapper(), Duration.ofHours(1),
            new GrpoEventPublisher(codec, (key, payload) -> sent.add(payload), true),
            offScorer());
        ServedMovie top = servedMovie("m1", false);

        sideEffects.recordServed(new ServingSideEffectRequest(
            "req-served-42", "u1", "ucb", Map.of(), List.of(top), List.of(top),
            List.of(), List.of(), 1, 0, 1, 0.8, 0.1, 0.5));

        ArgumentCaptor<String> pendingPayload = ArgumentCaptor.forClass(String.class);
        verify(valueOps).set(eq(MovieLensServingSideEffects.pendingReplayKey("u1", "m1")),
            pendingPayload.capture(), eq(Duration.ofHours(1)));
        Map<?, ?> replay = new ObjectMapper().readValue(pendingPayload.getValue(), Map.class);

        assertEquals(1, sent.size());
        byte[] payload = sent.get(0);
        var decoder = DecoderFactory.get().binaryDecoder(payload, 10, payload.length - 10, null);
        GenericRecord event = new GenericDatumReader<GenericRecord>(codec.schema()).read(null, decoder);

        assertEquals("req-served-42", replay.get(ReplayEvent.REQUEST_ID));
        assertEquals(replay.get(ReplayEvent.REQUEST_ID), event.get("request_id").toString());
    }

    private ServedMovie servedMovie(String movieId, boolean coldStart) {
        Map<String, Object> predictions = Map.of(
            "outcomeProbability", 0.7,
            "predictionScore", 0.8,
            "diversityScore", 1.0
        );
        return new ServedMovie(movieId, 0.8, 0.6, 0.1, 0.9, coldStart, 2, 1, predictions);
    }
}
