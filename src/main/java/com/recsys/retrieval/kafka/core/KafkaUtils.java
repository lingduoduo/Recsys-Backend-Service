package com.demo.retrieval.kafka.core;

import com.demo.retrieval.kafka.config.Args;
import com.demo.retrieval.kafka.config.KafkaConfig;
import com.demo.retrieval.kafka.config.KafkaConsumerConfig;
import com.demo.retrieval.kafka.config.KafkaProducerConfig;
import com.demo.retrieval.kafka.config.SslConfig;
import com.demo.retrieval.kafka.config.WilyConfig;
import com.demo.retrieval.kafka.event.KafkaMessage;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

// Shared factory methods for Kafka config objects — eliminates repeated builder chains for
// SSL, base KafkaConfig, consumer, and producer configs across startup paths.
public final class KafkaUtils {

    private KafkaUtils() {}

    public static SslConfig consumerSsl(Args args, String saslPassword) {
        return SslConfig.builder()
            .securityProtocol(args.getSecurityProtocol())
            .saslMechanism(args.getSaslMechanism())
            .saslUsername(args.getSaslUsername())
            .saslPassword(saslPassword)
            .build();
    }

    public static SslConfig producerSsl(Args args, String saslPassword) {
        return SslConfig.builder()
            .securityProtocol(args.getSecurityProtocol())
            .saslMechanism(args.getProducerSaslMechanism())
            .saslUsername(args.getProducerSaslUsername())
            .saslPassword(saslPassword)
            .build();
    }

    public static KafkaConfig kafkaConfig(String dest, String topic, SslConfig ssl) {
        return KafkaConfig.builder()
            .dest(dest)
            .topic(topic)
            .wilyConfig(WilyConfig.defaultConfig())
            .ssl(ssl)
            .build();
    }

    public static KafkaConsumerConfig consumerConfig(
            KafkaConfig base,
            String groupId,
            Args args,
            boolean enableAutoCommit,
            int maxPartitionFetchBytes) {
        return KafkaConsumerConfig.builder()
            .baseConfig(base)
            .groupId(groupId)
            .autoOffsetReset(args.getAutoOffsetReset())
            .enableAutoCommit(enableAutoCommit)
            .fetchTimeoutMs(args.getFetchTimeoutMs())
            .maxPartitionFetchBytes(maxPartitionFetchBytes)
            .skipToLatest(args.isSkipToLatest())
            .build();
    }

    public static KafkaProducerConfig producerConfig(KafkaConfig base) {
        return KafkaProducerConfig.builder()
            .baseConfig(base)
            .build();
    }

    public static MessageSource createKafkaConsumer(KafkaConsumerConfig config) {
        return new KafkaConsumer(config);
    }

    public static MessageSource localMessageSource(Path path, int batchSize) {
        return new LocalFileMessageSource(path, batchSize);
    }

    public static <T> List<T> deserializeKafkaMessages(
            List<KafkaMessage> messages, Function<byte[], T> deserializer) {
        return messages.stream()
            .map(m -> deserializer.apply(m.payload()))
            .toList();
    }
}
