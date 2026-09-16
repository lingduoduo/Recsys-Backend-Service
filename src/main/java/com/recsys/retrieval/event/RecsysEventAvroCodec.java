package com.demo.retrieval.event;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

/**
 * Avro single-object encoding for serving's own events, matching the wire format
 * EventAvroCodec (spark-streaming-job) and event_avro.py (python-modeling) both use:
 * 0xC3 0x01 + the writer schema's 64-bit fingerprint (little-endian) + a binary-encoded record.
 *
 * The Java and Scala modules do not share a classpath, so nothing enforces that a fingerprint
 * computed here matches one computed there except using the identical Avro library version
 * (1.11.3, pinned in pom.xml to match build.sbt): SchemaNormalization.parsingFingerprint64 then
 * agrees by construction rather than by cross-module agreement.
 */
public final class RecsysEventAvroCodec {

    private static final byte[] MAGIC = {(byte) 0xC3, 0x01};
    private static final String SCHEMA_RESOURCE = "/schemas/recsys-event-v3.avsc";

    private final Schema schema;
    private final long fingerprint;

    public RecsysEventAvroCodec() {
        this.schema = loadSchema(SCHEMA_RESOURCE);
        this.fingerprint = SchemaNormalization.parsingFingerprint64(schema);
    }

    public Schema schema() {
        return schema;
    }

    public long fingerprint() {
        return fingerprint;
    }

    /**
     * Encodes an event map as a single-object Avro payload. A field absent from the map takes the
     * schema's default (null for every field but the four required ones) exactly the way Python's
     * encode_event fills gaps from writer_schema's fields — callers only supply what they have, not
     * all 32 fields. A record missing one of the four fields with no default (event_id, user_id,
     * event_type, timestamp_ms) fails here with an AvroRuntimeException from GenericRecordBuilder.
     */
    public byte[] encode(Map<String, Object> event) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Map.Entry<String, Object> entry : event.entrySet()) {
            Schema.Field field = schema.getField(entry.getKey());
            if (field != null) {
                builder.set(field, entry.getValue());
            }
        }
        GenericRecord record = builder.build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(MAGIC);
            out.write(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(fingerprint).array());
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    private static Schema loadSchema(String resource) {
        try (InputStream input = RecsysEventAvroCodec.class.getResourceAsStream(resource)) {
            if (input == null) {
                throw new IllegalStateException("missing Avro schema resource " + resource);
            }
            return new Schema.Parser().parse(input);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
