package com.demo.retrieval.kafka.config;

import java.util.List;

public record KafkaConsumerConfig(
        KafkaConfig baseConfig,
        String groupId,
        String autoOffsetReset,
        boolean enableAutoCommit,
        long fetchTimeoutMs,
        int maxPartitionFetchBytes,
        boolean skipToLatest,
        List<Integer> partitions) {

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private KafkaConfig baseConfig;
        private String groupId, autoOffsetReset;
        private boolean enableAutoCommit = true;
        private long fetchTimeoutMs;
        private int maxPartitionFetchBytes;
        private boolean skipToLatest;
        private List<Integer> partitions;

        public Builder baseConfig(KafkaConfig v)          { baseConfig = v;             return this; }
        public Builder groupId(String v)                  { groupId = v;                return this; }
        public Builder autoOffsetReset(String v)          { autoOffsetReset = v;        return this; }
        public Builder enableAutoCommit(boolean v)        { enableAutoCommit = v;       return this; }
        public Builder fetchTimeoutMs(long v)             { fetchTimeoutMs = v;         return this; }
        public Builder maxPartitionFetchBytes(int v)      { maxPartitionFetchBytes = v; return this; }
        public Builder skipToLatest(boolean v)            { skipToLatest = v;           return this; }
        public Builder partitions(List<Integer> v)        { partitions = v;             return this; }

        public KafkaConsumerConfig build() {
            return new KafkaConsumerConfig(
                baseConfig, groupId, autoOffsetReset, enableAutoCommit,
                fetchTimeoutMs, maxPartitionFetchBytes, skipToLatest, partitions);
        }
    }

    public KafkaConsumerConfig withPartitions(List<Integer> newPartitions) {
        return new KafkaConsumerConfig(
            baseConfig, groupId, autoOffsetReset, enableAutoCommit,
            fetchTimeoutMs, maxPartitionFetchBytes, skipToLatest, newPartitions);
    }
}
