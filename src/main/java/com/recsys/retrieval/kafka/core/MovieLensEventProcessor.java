package com.demo.retrieval.kafka.core;

import com.demo.retrieval.kafka.config.Args;
import com.demo.retrieval.kafka.config.KafkaConsumerConfig;
import com.demo.retrieval.kafka.config.KafkaProducerConfig;
import com.demo.retrieval.kafka.event.KafkaMessage;
import com.demo.retrieval.kafka.event.MovieEvent;
import com.demo.retrieval.kafka.event.MovieInteractionEvent;
import com.demo.retrieval.kafka.event.MovieLensEvent;
import com.demo.retrieval.kafka.event.RatingEvent;
import com.demo.retrieval.kafka.event.RecSysEvent;
import com.demo.retrieval.kafka.event.UserEvent;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MovieLensEventProcessor {

    private static final AtomicInteger BATCH_LOG_COUNTER = new AtomicInteger(0);

    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduler;

    public MovieLensEventProcessor(int numThreads) {
        this.executorService = Executors.newFixedThreadPool(numThreads);
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    public void startProcessing(
            KafkaConsumerConfig baseConfig,
            KafkaProducerConfig producerConfig,
            Args args) {
        List<Integer> partitions = new ArrayList<>();
        for (int i = 0; i < args.getNumPartitions(); i++) {
            partitions.add(i);
        }

        KafkaProducer producer = new KafkaProducer(producerConfig);
        producer.start();

        spawnProcessingThreads(baseConfig, partitions, producer, args);
    }

    private void spawnProcessingThreads(
            KafkaConsumerConfig baseConfig,
            List<Integer> partitions,
            KafkaProducer producer,
            Args args) {
        int partitionsPerThread =
                (int) Math.ceil((double) partitions.size() / args.getKafkaNumThreads());

        for (int threadId = 0; threadId < args.getKafkaNumThreads(); threadId++) {
            int start = threadId * partitionsPerThread;
            int end = Math.min(start + partitionsPerThread, partitions.size());

            if (start >= partitions.size()) {
                break;
            }

            final int tid = threadId;
            KafkaConsumerConfig threadConfig =
                    baseConfig.withPartitions(new ArrayList<>(partitions.subList(start, end)));

            executorService.submit(() -> {
                try {
                    MessageSource consumer = KafkaUtils.createKafkaConsumer(threadConfig);
                    startPartitionLagMonitor(
                            consumer,
                            threadConfig.baseConfig().topic(),
                            args.getLagMonitorIntervalSecs());
                    processMovieLensEvents(consumer, producer, args.getKafkaBatchSize());
                } catch (Exception e) {
                    throw new RuntimeException(
                            "MovieLens event processing thread failed: " + tid, e);
                }
            });
        }
    }

    public void startLocalProcessing(Path dataFile, KafkaProducerConfig producerConfig, Args args) {
        KafkaProducer producer = new KafkaProducer(producerConfig);
        producer.start();
        MessageSource source = KafkaUtils.localMessageSource(dataFile, args.getKafkaBatchSize());
        executorService.submit(() -> {
            try {
                processMovieLensEvents(source, producer, args.getKafkaBatchSize());
            } catch (Exception e) {
                throw new RuntimeException("Local file processing failed: " + dataFile, e);
            }
        });
    }

    private void processMovieLensEvents(
            MessageSource source,
            KafkaProducer producer,
            int batchSize) throws Exception {
        List<KafkaMessage> buffer = new ArrayList<>();
        int batchNum = 0;
        int consecutiveErrors = 0;

        while (true) {
            try {
                List<KafkaMessage> polled = source.poll(100);
                consecutiveErrors = 0;
                buffer.addAll(polled);

                boolean flush = buffer.size() >= batchSize
                        || (source.isFinite() && polled.isEmpty() && !buffer.isEmpty());
                if (flush) {
                    List<KafkaMessage> batch = buffer;
                    buffer = new ArrayList<>(batchSize);
                    processBatch(batch, ++batchNum, producer);
                    source.commitOffsets();
                }

                if (source.isFinite() && polled.isEmpty() && buffer.isEmpty()) {
                    break;
                }
            } catch (Exception e) {
                Metrics.KAFKA_POLL_ERRORS.inc();
                consecutiveErrors++;
                long ceiling = Math.min(30_000L, 100L * (1L << Math.min(consecutiveErrors, 8)));
                Thread.sleep(ThreadLocalRandom.current().nextLong(ceiling + 1));
            }
        }
    }

    private void processBatch(
            List<KafkaMessage> messages,
            int batchNum,
            KafkaProducer producer) throws Exception {
        List<MovieLensEvent> events =
                KafkaUtils.deserializeKafkaMessages(messages, MovieLensDeserializer::deserialize);

        for (MovieLensEvent event : events) {
            RecSysEvent out;
            if (event instanceof UserEvent e)                  out = handleUserEvent(e);
            else if (event instanceof MovieEvent e)            out = handleMovieEvent(e);
            else if (event instanceof RatingEvent e)           out = handleRatingEvent(e);
            else if (event instanceof MovieInteractionEvent e) out = handleInteractionEvent(e);
            else continue;

            if (out instanceof RecSysEvent.InteractionCreated) {
                producer.send(KafkaTopics.USER_EVENTS, out.toByteArray());
            } else if (out instanceof RecSysEvent.RatingCreated) {
                producer.send(KafkaTopics.BEHAVIOR_LOGS, out.toByteArray());
                producer.send(KafkaTopics.MOVIELENS_CONTEXT, out.toByteArray());
            } else if (out instanceof RecSysEvent.UserUpdated || out instanceof RecSysEvent.MovieUpdated) {
                producer.send(KafkaTopics.MOVIELENS_CONTEXT, out.toByteArray());
            }
        }

        int batchCount = BATCH_LOG_COUNTER.incrementAndGet();
        if (batchCount % 1000 == 0) {
            System.out.printf(
                    "Processed %d MovieLens batches, latest batch=%d, events=%d%n",
                    batchCount, batchNum, events.size());
        }
    }

    private RecSysEvent handleUserEvent(UserEvent event) {
        return RecSysEvent.userUpdated(
                event.getUserId(), event.getAge(), event.getGender(),
                event.getOccupation(), event.getZipCode(), event.getTimestamp());
    }

    private RecSysEvent handleMovieEvent(MovieEvent event) {
        return RecSysEvent.movieUpdated(
                event.getMovieId(), event.getTitle(), event.getGenres(),
                event.getReleaseYear(), event.getTimestamp());
    }

    private RecSysEvent handleRatingEvent(RatingEvent event) {
        return RecSysEvent.ratingCreated(
                event.getUserId(), event.getMovieId(),
                event.getRating(), event.getTimestamp());
    }

    private RecSysEvent handleInteractionEvent(MovieInteractionEvent event) {
        return RecSysEvent.interactionCreated(
                event.getUserId(), event.getMovieId(),
                event.getEventType(), event.getTimestamp());
    }

    private void startPartitionLagMonitor(
            MessageSource source,
            String topic,
            long intervalSecs) {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                for (PartitionLag lag : source.getPartitionLags()) {
                    Metrics.KAFKA_PARTITION_LAG
                            .labels(topic, String.valueOf(lag.getPartitionId()))
                            .set(lag.getLag());
                }
            } catch (Exception e) {
                System.err.println("Failed to monitor partition lag: " + e.getMessage());
            }
        }, 0, intervalSecs, TimeUnit.SECONDS);
    }
}
