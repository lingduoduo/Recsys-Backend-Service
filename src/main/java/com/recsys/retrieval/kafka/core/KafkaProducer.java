package com.demo.retrieval.kafka.core;

import com.demo.retrieval.kafka.config.KafkaProducerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Logger;

public class KafkaProducer {

    private static final Logger LOG = Logger.getLogger(KafkaProducer.class.getName());

    private final String defaultTopic;
    private final org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> delegate;

    public KafkaProducer(KafkaProducerConfig config) {
        this.defaultTopic = config.baseConfig().topic();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.baseConfig().dest());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        this.delegate = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    public void start() {}

    public void send(byte[] payload) {
        send(defaultTopic, payload);
    }

    public void send(String topic, byte[] payload) {
        delegate.send(new ProducerRecord<>(topic, payload), (metadata, ex) -> {
            if (ex != null) {
                LOG.warning("Failed to send to topic=" + topic + ": " + ex.getMessage());
            }
        });
    }

    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }
}
