package com.demo.retrieval.kafka.config;

public record KafkaProducerConfig(KafkaConfig baseConfig) {

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private KafkaConfig baseConfig;

        public Builder baseConfig(KafkaConfig v) { baseConfig = v; return this; }

        public KafkaProducerConfig build() { return new KafkaProducerConfig(baseConfig); }
    }
}
