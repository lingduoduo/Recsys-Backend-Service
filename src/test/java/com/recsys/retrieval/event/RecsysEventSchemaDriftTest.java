package com.recsys.retrieval.event;

import com.recsys.retrieval.support.ContractFixtures;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This frozen producer-contract snapshot stays in the service so its tests do not require the
 * pipeline checkout. The pipeline-owned contract comparison verifies it against the canonical
 * producer schema. Fingerprint equality is what the event decoder keys its writer-schema lookup
 * on, so formatting differences do not affect wire compatibility.
 */
class RecsysEventSchemaDriftTest {

    @Test
    void bundledSchemaFingerprintMatchesTheFrozenContractSnapshot() {
        Schema snapshot = new Schema.Parser().parse(ContractFixtures.text("recsys-event-v3.avsc"));

        RecsysEventAvroCodec codec = new RecsysEventAvroCodec();

        assertEquals(SchemaNormalization.parsingFingerprint64(snapshot), codec.fingerprint());
    }
}
