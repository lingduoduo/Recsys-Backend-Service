package com.demo.retrieval.kafka;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// Reads item_embedding_sample.txt lines of the form "itemId:v1 v2 v3 ..."
// and returns an immutable map from item ID to its embedding vector.
public final class LocalEmbeddingLoader {

    private LocalEmbeddingLoader() {}

    public static Map<String, float[]> load(Path path) {
        try (Stream<String> lines = Files.lines(path)) {
            return lines
                .filter(line -> !line.isBlank())
                .collect(Collectors.toUnmodifiableMap(
                    line -> line.substring(0, line.indexOf(':')),
                    line -> parseVector(line.substring(line.indexOf(':') + 1))
                ));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot load embeddings from: " + path, e);
        }
    }

    private static float[] parseVector(String values) {
        String[] parts = values.trim().split("\\s+");
        float[] vec = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            vec[i] = Float.parseFloat(parts[i]);
        }
        return vec;
    }
}
