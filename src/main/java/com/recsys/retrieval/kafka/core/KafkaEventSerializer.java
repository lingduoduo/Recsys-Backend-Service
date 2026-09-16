package com.demo.retrieval.kafka.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public final class KafkaEventSerializer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private KafkaEventSerializer() {}

    public static byte[] toJsonBytes(Map<String, Object> fields) {
        try {
            return MAPPER.writeValueAsBytes(fields);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize event to JSON", e);
        }
    }
}
