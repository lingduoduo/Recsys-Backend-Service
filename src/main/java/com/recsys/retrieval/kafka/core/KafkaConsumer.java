package com.demo.retrieval.kafka.core;

import com.demo.retrieval.kafka.config.KafkaConsumerConfig;
import com.demo.retrieval.kafka.config.SslConfig;
import com.demo.retrieval.kafka.event.KafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer implements MessageSource {

    private final org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> delegate;

    public KafkaConsumer(KafkaConsumerConfig config) {
        this.delegate = new org.apache.kafka.clients.consumer.KafkaConsumer<>(buildProperties(config));
        initAssignment(config);
    }

    private static Properties buildProperties(KafkaConsumerConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.baseConfig().dest());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.autoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(config.enableAutoCommit()));
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(config.fetchTimeoutMs()));
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(config.maxPartitionFetchBytes()));
        SslConfig ssl = config.baseConfig().ssl();
        if (ssl != null && ssl.securityProtocol() != null) {
            props.put("security.protocol", ssl.securityProtocol());
            props.put("sasl.mechanism", ssl.saslMechanism());
            props.put("sasl.jaas.config", String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                    "username=\"%s\" password=\"%s\";",
                    ssl.saslUsername(), ssl.saslPassword()));
        }
        return props;
    }

    private void initAssignment(KafkaConsumerConfig config) {
        String topic = config.baseConfig().topic();
        List<Integer> partitionIds = config.partitions();
        if (partitionIds != null && !partitionIds.isEmpty()) {
            List<TopicPartition> topicPartitions = partitionIds.stream()
                    .map(p -> new TopicPartition(topic, p))
                    .toList();
            delegate.assign(topicPartitions);
            if (config.skipToLatest()) {
                delegate.seekToEnd(topicPartitions);
            }
        } else {
            delegate.subscribe(List.of(topic));
        }
    }

    @Override
    public List<KafkaMessage> poll(int timeoutMs) {
        ConsumerRecords<String, byte[]> records = delegate.poll(Duration.ofMillis(timeoutMs));
        List<KafkaMessage> messages = new ArrayList<>(records.count());
        for (ConsumerRecord<String, byte[]> record : records) {
            messages.add(new KafkaMessage(record.value(), record.partition(), record.offset()));
        }
        return messages;
    }

    @Override
    public void commitOffsets() {
        delegate.commitSync();
    }

    @Override
    public List<PartitionLag> getPartitionLags() {
        List<TopicPartition> assigned = new ArrayList<>(delegate.assignment());
        if (assigned.isEmpty()) return List.of();
        Map<TopicPartition, Long> endOffsets = delegate.endOffsets(assigned);
        List<PartitionLag> lags = new ArrayList<>(assigned.size());
        for (TopicPartition tp : assigned) {
            long end = endOffsets.getOrDefault(tp, 0L);
            long position = delegate.position(tp);
            lags.add(new PartitionLag(tp.partition(), Math.max(0L, end - position)));
        }
        return lags;
    }
}
