package com.demo.retrieval.service;

import com.demo.retrieval.model.ModelPrediction;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.LongBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class DeepLearningPredictionService {
    private static final String MODEL_NAME = "mlp_embedding";
    private static final String MODEL_RESOURCE = "mlp_embedding_model.onnx";
    private static final String LOOKUP_RESOURCE = "mlp_embedding_lookups.json";
    // Offline layer: set these env vars to load model artifacts from the filesystem
    // so models can be updated without redeployment. Falls back to classpath resources
    // when not set (used in development and tests).
    private static final String MODEL_PATH_ENV = "ONNX_MODEL_PATH";
    private static final String LOOKUPS_PATH_ENV = "ONNX_LOOKUPS_PATH";
    private static final String USER_INPUT = "user_ids";
    private static final String ITEM_INPUT = "item_ids";

    private final OrtEnvironment environment;
    private volatile OrtSession session;
    private final Map<String, Long> userLookup;
    private final Map<String, Long> itemLookup;

    public DeepLearningPredictionService(ObjectMapper objectMapper) {
        try {
            this.environment = OrtEnvironment.getEnvironment();
            try (OrtSession.SessionOptions opts = new OrtSession.SessionOptions()) {
                opts.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);
                this.session = environment.createSession(loadModelBytes(), opts);
            }
            LookupTables lookups = readLookups(objectMapper);
            this.userLookup = lookups.userLookup();
            this.itemLookup = lookups.itemLookup();
        } catch (IOException | OrtException e) {
            throw new IllegalStateException("Failed to load deep learning prediction artifacts", e);
        }
    }

    private static byte[] loadModelBytes() throws IOException {
        String path = System.getenv(MODEL_PATH_ENV);
        return path != null && !path.isBlank()
            ? Files.readAllBytes(Path.of(path))
            : new ClassPathResource(MODEL_RESOURCE).getContentAsByteArray();
    }

    public Optional<ModelPrediction> predict(String user, String item) {
        Long userId = userLookup.get(user);
        Long itemId = itemLookup.get(item);
        if (userId == null || itemId == null) {
            return Optional.empty();
        }
        return Optional.of(predict(user, item, userId, itemId));
    }

    public ModelPrediction predict(long userId, long itemId) {
        if (userId >= userLookup.size() || itemId >= itemLookup.size()) {
            throw new ModelIndexOutOfRangeException(
                "Model indices out of range: userId must be 0.." + (userLookup.size() - 1)
                    + " and itemId must be 0.." + (itemLookup.size() - 1)
            );
        }
        return predict(null, null, userId, itemId);
    }

    public Map<String, Object> metadata() {
        return Map.of(
            "model", MODEL_NAME,
            "users", userLookup.size(),
            "items", itemLookup.size(),
            "inputs", session.getInputNames(),
            "outputs", session.getOutputNames()
        );
    }

    @PreDestroy
    void close() throws OrtException {
        session.close();
    }

    public synchronized void reload() throws IOException, OrtException {
        try (OrtSession.SessionOptions opts = new OrtSession.SessionOptions()) {
            opts.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);
            OrtSession newSession = environment.createSession(loadModelBytes(), opts);
            OrtSession old = this.session;
            this.session = newSession;
            if (old != null) {
                old.close();
            }
        }
    }

    public Map<String, Double> predictBatch(String user, List<String> items) {
        Long userId = userLookup.get(user);
        if (userId == null || items.isEmpty()) {
            return Map.of();
        }
        List<String> known = items.stream().filter(itemLookup::containsKey).toList();
        if (known.isEmpty()) {
            return Map.of();
        }
        int n = known.size();
        long[] userBatch = new long[n];
        long[] itemBatch = new long[n];
        Arrays.fill(userBatch, userId);
        for (int i = 0; i < n; i++) {
            itemBatch[i] = itemLookup.get(known.get(i));
        }
        try (
            OnnxTensor userTensor = OnnxTensor.createTensor(environment, LongBuffer.wrap(userBatch), new long[]{n});
            OnnxTensor itemTensor = OnnxTensor.createTensor(environment, LongBuffer.wrap(itemBatch), new long[]{n});
            OrtSession.Result result = session.run(Map.of(USER_INPUT, userTensor, ITEM_INPUT, itemTensor))
        ) {
            double[] scores = readBatchScores(result.get(0), n);
            Map<String, Double> out = new HashMap<>(n * 4 / 3 + 1);
            for (int i = 0; i < n; i++) {
                out.put(known.get(i), scores[i]);
            }
            return Map.copyOf(out);
        } catch (OrtException e) {
            throw new IllegalStateException("Failed to run batch deep learning prediction", e);
        }
    }

    private ModelPrediction predict(String user, String item, long userId, long itemId) {
        try (
            OnnxTensor userTensor = OnnxTensor.createTensor(environment, LongBuffer.wrap(new long[]{userId}), new long[]{1});
            OnnxTensor itemTensor = OnnxTensor.createTensor(environment, LongBuffer.wrap(new long[]{itemId}), new long[]{1});
            OrtSession.Result result = session.run(Map.of(USER_INPUT, userTensor, ITEM_INPUT, itemTensor))
        ) {
            double score = readScore(result.get(0));
            return new ModelPrediction(user, item, userId, itemId, score, MODEL_NAME);
        } catch (OrtException e) {
            throw new IllegalStateException("Failed to run deep learning prediction", e);
        }
    }

    private double readScore(OnnxValue value) throws OrtException {
        return normalizeBatchScores(value.getValue(), 1)[0];
    }

    private double[] readBatchScores(OnnxValue value, int n) throws OrtException {
        return normalizeBatchScores(value.getValue(), n);
    }

    // Always returns exactly n scores: missing indices default to 0.0, extras are ignored, so a
    // model whose output length drifts from the batch size degrades gracefully instead of crashing
    // the recommend request with an ArrayIndexOutOfBounds.
    static double[] normalizeBatchScores(Object raw, int n) {
        double[] out = new double[n];
        if (raw instanceof float[][] s) {
            for (int i = 0; i < n && i < s.length; i++) out[i] = s[i].length > 0 ? s[i][0] : 0.0;
            return out;
        }
        if (raw instanceof float[] s) {
            for (int i = 0; i < n && i < s.length; i++) out[i] = s[i];
            return out;
        }
        if (raw instanceof double[][] s) {
            for (int i = 0; i < n && i < s.length; i++) out[i] = s[i].length > 0 ? s[i][0] : 0.0;
            return out;
        }
        if (raw instanceof double[] s) {
            for (int i = 0; i < n && i < s.length; i++) out[i] = s[i];
            return out;
        }
        throw new IllegalStateException("Unsupported batch prediction output shape: "
            + (raw == null ? "null" : raw.getClass().getName()));
    }

    private LookupTables readLookups(ObjectMapper objectMapper) throws IOException {
        String path = System.getenv(LOOKUPS_PATH_ENV);
        InputStream stream = path != null && !path.isBlank()
            ? Files.newInputStream(Path.of(path))
            : new ClassPathResource(LOOKUP_RESOURCE).getInputStream();
        Map<String, Map<String, Long>> raw = objectMapper.readValue(stream, new TypeReference<>() {});
        return new LookupTables(
            Map.copyOf(raw.getOrDefault("user_lookup", new LinkedHashMap<>())),
            Map.copyOf(raw.getOrDefault("item_lookup", new LinkedHashMap<>()))
        );
    }

    private record LookupTables(Map<String, Long> userLookup, Map<String, Long> itemLookup) {
    }
}
