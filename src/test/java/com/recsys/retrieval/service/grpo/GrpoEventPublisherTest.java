package com.demo.retrieval.service.grpo;

import com.demo.retrieval.event.RecsysEventAvroCodec;
import com.demo.retrieval.model.FeedbackRequest;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpoEventPublisherTest {

    private final RecsysEventAvroCodec codec = new RecsysEventAvroCodec();
    private final List<byte[]> sent = new ArrayList<>();
    private final GrpoEventPublisher.GrpoSender recorder = (key, payload) -> sent.add(payload);

    /**
     * Decodes a payload the same way OnlineJoinerStreamingJob's real decoder,
     * EventAvroCodec.decode, does: magic + little-endian fingerprint header, then a binary Avro
     * body. Verifying against the real wire format here is the point of this suite — a regression
     * back to raw JSON must fail these assertions the way it silently dead-lettered in production.
     */
    private GenericRecord decode(byte[] payload) throws IOException {
        assertEquals(codec.fingerprint(),
            ByteBuffer.wrap(payload, 2, 8).order(ByteOrder.LITTLE_ENDIAN).getLong());
        var decoder = DecoderFactory.get().binaryDecoder(payload, 10, payload.length - 10, null);
        return new GenericDatumReader<GenericRecord>(codec.schema()).read(null, decoder);
    }

    private ServingSideEffectRequest request() {
        ServedMovie movie = new ServedMovie("m1", 0.4, 0.3, 0.05, 0.7, false, 10, 2,
            Map.of("predictionScore", 0.7));
        return new ServingSideEffectRequest(
            "req-1", "u1", "hybrid", Map.of(), List.of(movie), List.of(movie),
            List.of(), List.of(), 0L, 0L, 1L, 0.0, 0.0, 0.0);
    }

    @Test
    void sendsNothingWhenDisabled() {
        new GrpoEventPublisher(codec, recorder, false).publish(request(), 1000L);
        assertTrue(sent.isEmpty());
    }

    @Test
    void sendsOneMessagePerItemWhenEnabled() throws IOException {
        new GrpoEventPublisher(codec, recorder, true).publish(request(), 1000L);
        assertEquals(1, sent.size());
        GenericRecord decoded = decode(sent.get(0));
        assertEquals("m1", decoded.get("item_id").toString());
        // Avro's GenericDatumReader returns map keys as Utf8, not String, so look up by content.
        @SuppressWarnings("unchecked")
        Map<Object, Object> itemFeatures = (Map<Object, Object>) decoded.get("item_features");
        String grpoX = itemFeatures.entrySet().stream()
            .filter(entry -> entry.getKey().toString().equals("grpo_x"))
            .map(entry -> entry.getValue().toString())
            .findFirst()
            .orElseThrow();
        assertTrue(grpoX.startsWith("v2:"), grpoX);
    }

    @Test
    void aSenderFailureNeverPropagates() {
        GrpoEventPublisher.GrpoSender broken = (key, payload) -> {
            throw new IllegalStateException("broker down");
        };
        // A Kafka outage must not fail a recommendation request. This is a logging side effect.
        new GrpoEventPublisher(codec, broken, true).publish(request(), 1000L);
    }

    @Test
    void publishFeedbackSendsNothingWhenDisabled() {
        FeedbackRequest feedback = new FeedbackRequest("u1", "m1", true, 1.0);
        new GrpoEventPublisher(codec, recorder, false).publishFeedback(feedback, "req-1", 1000L);
        assertTrue(sent.isEmpty());
    }

    @Test
    void publishFeedbackSendsOneMessageOnClickWhenEnabled() throws IOException {
        FeedbackRequest feedback = new FeedbackRequest("u1", "m1", true, 1.0);
        new GrpoEventPublisher(codec, recorder, true).publishFeedback(feedback, "req-1", 1000L);
        assertEquals(1, sent.size());
        GenericRecord decoded = decode(sent.get(0));
        assertEquals("click", decoded.get("event_type").toString());
        assertEquals("req-1", decoded.get("request_id").toString());
    }

    @Test
    void publishFeedbackSendsNothingWithoutAClick() {
        FeedbackRequest feedback = new FeedbackRequest("u1", "m1", false, 0.0);
        new GrpoEventPublisher(codec, recorder, true).publishFeedback(feedback, "req-1", 1000L);
        assertTrue(sent.isEmpty());
    }

    @Test
    void publishFeedbackSenderFailureNeverPropagates() {
        GrpoEventPublisher.GrpoSender broken = (key, payload) -> {
            throw new IllegalStateException("broker down");
        };
        FeedbackRequest feedback = new FeedbackRequest("u1", "m1", true, 1.0);
        // A Kafka outage must not fail a feedback request either. Same logging side effect.
        new GrpoEventPublisher(codec, broken, true).publishFeedback(feedback, "req-1", 1000L);
    }
}
