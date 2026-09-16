package com.demo.retrieval.kafka.event;

public record KafkaMessage(byte[] payload, int partition, long offset) {}
