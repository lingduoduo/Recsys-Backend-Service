package com.recsys.retrieval.kafka.core;

import com.recsys.retrieval.kafka.config.KafkaConfig;
import com.recsys.retrieval.kafka.config.KafkaProducerConfig;
import com.recsys.retrieval.kafka.config.SslConfig;
import com.recsys.retrieval.kafka.config.WilyConfig;

// Shared factory methods for Kafka producer config objects.
//
// The consumer-side helpers (consumerSsl, consumerConfig, createKafkaConsumer,
// localMessageSource, deserializeKafkaMessages) were removed when this tree was consolidated
// into model serving: nothing outside kafka/ ever called them, and they were the only thing
// keeping KafkaConsumer, KafkaConsumerConfig, MessageSource, LocalFileMessageSource,
// PartitionLag, KafkaMessage and Args reachable. Consuming in this system is
// infrastructure/messaging's job.
public final class KafkaUtils {

    private KafkaUtils() {}

    public static KafkaConfig kafkaConfig(String dest, String topic, SslConfig ssl) {
        return KafkaConfig.builder()
            .dest(dest)
            .topic(topic)
            .wilyConfig(WilyConfig.defaultConfig())
            .ssl(ssl)
            .build();
    }

    public static KafkaProducerConfig producerConfig(KafkaConfig base) {
        return KafkaProducerConfig.builder()
            .baseConfig(base)
            .build();
    }
}
