package com.demo.retrieval.event;

import com.demo.retrieval.support.ContractFixtures;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * The Java retrieval-service module and spark-streaming-job do not share a classpath, so the only
 * way to prove serving's encoder and the joiner's decoder agree is to hand real bytes between
 * them. This test encodes a representative serving impression event and checks it against a
 * frozen producer-contract snapshot; a companion Scala test in spark-streaming-job decodes that
 * fixture through the real production decode path, EventAvroCodec.decode. The pipeline-owned
 * contract comparison verifies this snapshot against the canonical producer fixture. Drift in this
 * encoder — the same class of bug that shipped raw JSON to an Avro decoder — fails this comparison
 * instead of silently dead-lettering in production.
 */
class RecsysEventFixtureTest {

    /** Field values here must match the companion Scala test's assertions exactly. */
    static Map<String, Object> representativeImpressionEvent() {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("event_id", "e-fixture-1");
        event.put("request_id", "req-fixture-1");
        event.put("session_id", "sess-fixture-1");
        event.put("user_id", "u-fixture-1");
        event.put("item_id", "m-fixture-1");
        event.put("event_type", "impression");
        event.put("timestamp_ms", 1735689600000L);
        event.put("position", 0);
        event.put("user_features", Map.of("algorithm", "hybrid"));
        Map<String, String> itemFeatures = new LinkedHashMap<>();
        itemFeatures.put("prediction_score", "0.83");
        // Frozen codec fixture, not a live GrpoFeatures sample: grpo_x is an opaque
        // map<string,string> value to this byte-identity check, and changing this v1/10-wide
        // string would require regenerating the committed .avro fixture below for no encoding
        // benefit -- GrpoFeatures has since moved to v2/9-wide.
        itemFeatures.put("grpo_x", "v1:1.0,0.4,0.3,0.05,0.7,0.0,0.0,0.0,0.0,0.3");
        event.put("item_features", itemFeatures);
        event.put("context_features", Map.of());
        return event;
    }

    @Test
    void encodingIsByteIdenticalToTheFrozenContractSnapshot() {
        byte[] encoded = new RecsysEventAvroCodec().encode(representativeImpressionEvent());
        byte[] expected = ContractFixtures.bytes("serving-impression-v3.avro");

        assertArrayEquals(expected, encoded);
    }
}
