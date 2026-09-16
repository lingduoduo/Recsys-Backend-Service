package com.demo.retrieval.kafka.config;

public record KafkaConfig(
        String dest,
        String topic,
        WilyConfig wilyConfig,
        SslConfig ssl) {

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String dest, topic;
        private WilyConfig wilyConfig;
        private SslConfig ssl;

        public Builder dest(String v)           { dest = v;        return this; }
        public Builder topic(String v)          { topic = v;       return this; }
        public Builder wilyConfig(WilyConfig v) { wilyConfig = v;  return this; }
        public Builder ssl(SslConfig v)         { ssl = v;         return this; }

        public KafkaConfig build() {
            return new KafkaConfig(dest, topic, wilyConfig, ssl);
        }
    }
}
