package com.demo.retrieval.kafka.core;

public class Metrics {

    public static final Counter KAFKA_POLL_ERRORS = new Counter();
    public static final GaugeVec KAFKA_PARTITION_LAG = new GaugeVec();

    public static class Counter {
        public void inc() {}
    }

    public static class GaugeVec {
        public Gauge labels(String... labelValues) { return new Gauge(); }
    }

    public static class Gauge {
        public void set(double value) {}
    }
}
