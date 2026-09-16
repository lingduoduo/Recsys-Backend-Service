package com.recsys.retrieval.kafka.event;

public record KafkaMessage(byte[] payload, int partition, long offset) {}
