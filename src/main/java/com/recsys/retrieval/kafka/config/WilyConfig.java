package com.recsys.retrieval.kafka.config;

public record WilyConfig() {
    public static WilyConfig defaultConfig() { return new WilyConfig(); }
}
