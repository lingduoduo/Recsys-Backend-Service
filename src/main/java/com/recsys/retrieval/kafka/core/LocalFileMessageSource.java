package com.demo.retrieval.kafka.core;

import com.demo.retrieval.kafka.event.KafkaMessage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

// Reads a local file line-by-line and exposes each line as a KafkaMessage payload.
// Skips the first line if it looks like a CSV header (contains no digits in field 0).
public class LocalFileMessageSource implements MessageSource {

    private final BufferedReader reader;
    private final int batchSize;
    private boolean done = false;

    public LocalFileMessageSource(Path path, int batchSize) {
        try {
            this.reader = Files.newBufferedReader(path);
            this.batchSize = batchSize;
            skipHeaderIfPresent();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open local data file: " + path, e);
        }
    }

    @Override
    public List<KafkaMessage> poll(int timeoutMs) {
        if (done) {
            return List.of();
        }
        List<KafkaMessage> batch = new ArrayList<>(batchSize);
        try {
            for (int i = 0; i < batchSize; i++) {
                String line = reader.readLine();
                if (line == null) {
                    done = true;
                    break;
                }
                batch.add(new KafkaMessage(line.getBytes(), 0, i));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return batch;
    }

    @Override
    public void commitOffsets() {}

    @Override
    public List<PartitionLag> getPartitionLags() {
        return List.of();
    }

    @Override
    public boolean isFinite() { return true; }

    public boolean isDone() {
        return done;
    }

    private void skipHeaderIfPresent() throws IOException {
        reader.mark(256);
        String first = reader.readLine();
        if (first == null) {
            return;
        }
        // If the first field contains no digit it's a header row (e.g. "userId,movieId,...")
        String firstField = first.split(",")[0];
        if (firstField.chars().noneMatch(Character::isDigit)) {
            return; // consumed — header skipped
        }
        reader.reset(); // not a header — push the line back
    }
}
