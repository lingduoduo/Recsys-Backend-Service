package com.recsys.application.retrieval;
import com.recsys.application.model.ScoredItems;
import com.recsys.application.experiment.ModelVariants;
import com.recsys.application.feature.FeatureEncoder;
import com.recsys.application.model.ModelArtifactLocator;
import com.recsys.application.model.ModelArtifactManifest.ModelContract;
import com.recsys.application.model.ModelArtifactManifest.TensorType;
import com.recsys.application.model.Strings;
import com.recsys.config.ModelServingProperties;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxJavaType;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.TensorInfo;
import ai.onnxruntime.ValueInfo;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import com.recsys.domain.prediction.ScoredItem;

/**
 * Owns one ONNX session for one model variant. {@link #init()} is transactional: the session
 * is opened, its input/output metadata is checked against the {@link ModelContract}, and one
 * smoke inference must return exactly one finite score before {@link #isReady()} becomes true.
 * Any failure closes whatever was opened and leaves the service not ready.
 */
public class UserTowerInferenceService {

    private static final String DEFAULT_MODEL_FILE = "dssm_model.onnx";

    /**
     * The one-row batch the smoke inference submits. Derived from the bundle's own feature
     * config, because "row 0" is a fact about the bundled demo vocabularies, not a contract: a
     * production bundle whose {@code __UNK__} row is not 0 must be smoked on its own unknown row.
     * Items have no unknown row (out-of-vocab items are never scored), so the smallest item index
     * is used — it is valid for any non-empty vocabulary.
     */
    public record SmokeInputs(long userIndex, long itemIndex) {
        /** Row 0 / row 0 — what the legacy locator constructors use when no feature config is in hand. */
        public static final SmokeInputs DEFAULT = new SmokeInputs(0L, 0L);

        public static SmokeInputs from(com.recsys.application.model.ModelArtifactService artifactService) {
            Map<String, Integer> users = artifactService.getUserVocab();
            Integer unknown = users.get(FeatureEncoder.UNKNOWN_USER);
            long user = unknown != null ? unknown : smallest(users);
            return new SmokeInputs(user, smallest(artifactService.getItemVocab()));
        }

        private static long smallest(Map<String, Integer> vocab) {
            return vocab.values().stream().mapToLong(Integer::longValue).min().orElse(0L);
        }
    }

    private final ModelArtifactLocator artifactLocator;
    private final String variant;
    private final String modelFile;
    /** Nulled at the end of {@link #init()}: ONNX Runtime holds its own native copy, so keeping ours doubles the model's heap. */
    private byte[] modelBytes;
    private final ModelContract contract;
    private final ModelServingProperties.Onnx onnx;
    private final OnnxSessionFactory sessionFactory;
    private final SmokeInputs smokeInputs;
    private final Counter runCounter;
    private final AtomicLong runCount = new AtomicLong();
    private volatile OnnxSessionHandle session;
    private volatile boolean ready = false;

    /** Legacy: reads {@code dssm_model.onnx} through the locator with the legacy contract and default execution settings. */
    public UserTowerInferenceService(ModelArtifactLocator artifactLocator, String variant) {
        this(artifactLocator, variant, DEFAULT_MODEL_FILE);
    }

    /** Legacy: reads {@code modelFile} through the locator with the legacy contract and default execution settings. */
    public UserTowerInferenceService(ModelArtifactLocator artifactLocator, String variant, String modelFile) {
        this(Objects.requireNonNull(artifactLocator, "artifactLocator"),
                ModelVariants.trimOrEmpty(variant),
                Strings.orDefault(modelFile, DEFAULT_MODEL_FILE),
                null,
                ModelContract.legacy(),
                ModelServingProperties.Onnx.fromEnvironment(),
                new SimpleMeterRegistry(),
                OrtSessionHandle::open,
                SmokeInputs.DEFAULT);
    }

    /**
     * Verified-bytes constructor used by {@code ModelRuntimeProvider}: the caller has already
     * resolved (and, for manifest bundles, checksummed) the model bytes and its contract.
     */
    public UserTowerInferenceService(byte[] modelBytes,
                                     ModelContract contract,
                                     ModelServingProperties.Onnx onnx,
                                     String variant,
                                     MeterRegistry registry) {
        this(modelBytes, contract, onnx, variant, registry, SmokeInputs.DEFAULT);
    }

    /** As above, smoking the bundle's own unknown/first rows ({@link SmokeInputs#from}). */
    public UserTowerInferenceService(byte[] modelBytes,
                                     ModelContract contract,
                                     ModelServingProperties.Onnx onnx,
                                     String variant,
                                     MeterRegistry registry,
                                     SmokeInputs smokeInputs) {
        this(modelBytes, contract, onnx, variant, registry, OrtSessionHandle::open, smokeInputs);
    }

    UserTowerInferenceService(byte[] modelBytes,
                              ModelContract contract,
                              ModelServingProperties.Onnx onnx,
                              String variant,
                              MeterRegistry registry,
                              OnnxSessionFactory sessionFactory) {
        this(modelBytes, contract, onnx, variant, registry, sessionFactory, SmokeInputs.DEFAULT);
    }

    UserTowerInferenceService(byte[] modelBytes,
                              ModelContract contract,
                              ModelServingProperties.Onnx onnx,
                              String variant,
                              MeterRegistry registry,
                              OnnxSessionFactory sessionFactory,
                              SmokeInputs smokeInputs) {
        this(null, ModelVariants.trimOrEmpty(variant), DEFAULT_MODEL_FILE,
                Objects.requireNonNull(modelBytes, "modelBytes").clone(),
                contract, onnx, registry, sessionFactory, smokeInputs);
    }

    private UserTowerInferenceService(ModelArtifactLocator artifactLocator,
                                      String variant,
                                      String modelFile,
                                      byte[] modelBytes,
                                      ModelContract contract,
                                      ModelServingProperties.Onnx onnx,
                                      MeterRegistry registry,
                                      OnnxSessionFactory sessionFactory,
                                      SmokeInputs smokeInputs) {
        this.artifactLocator = artifactLocator;
        this.smokeInputs = Objects.requireNonNull(smokeInputs, "smokeInputs");
        this.variant = variant;
        this.modelFile = modelFile;
        this.modelBytes = modelBytes;
        this.contract = Objects.requireNonNull(contract, "contract");
        this.onnx = Objects.requireNonNull(onnx, "onnx");
        this.sessionFactory = Objects.requireNonNull(sessionFactory, "sessionFactory");
        // Tagged with the normalized variant: the label set is the closed configured variant
        // set, never a request-supplied value, so cardinality stays bounded.
        this.runCounter = Counter.builder("recsys.model.onnx.runs")
                .description("Native ONNX session runs, one per OrtSession.run call")
                .tag("variant", ModelVariants.normalizeOrDefault(variant))
                .register(Objects.requireNonNull(registry, "registry"));
    }

    public void init() throws Exception {
        byte[] bytes = modelBytes != null ? modelBytes : readLegacyModelBytes();
        modelBytes = null;   // the runtime is discarded on failure; on success the session has its own copy
        OnnxSessionHandle opened = sessionFactory.open(bytes, onnx);
        try {
            validateMetadata(opened);
            smokeTest(opened);
        } catch (Exception e) {
            opened.close();
            throw e;
        }
        this.session = opened;
        this.ready = true;
    }

    private byte[] readLegacyModelBytes() throws java.io.IOException {
        try {
            return artifactLocator.readModelBytes(variant, modelFile);
        } catch (IllegalStateException e) {
            throw new IllegalStateException(modelFile + " not found at "
                    + artifactLocator.describeModelLocation(variant, modelFile)
                    + ". Set recsys.model.artifacts-dir to an external model directory, or place artifacts under classpath:artifacts/model/<variant>/.", e);
        }
    }

    private void validateMetadata(OnnxSessionHandle handle) throws OrtException {
        Map<String, NodeInfo> inputs = handle.inputInfo();
        requireTensor(inputs, contract.userInput(), "input", contract.inputType(), contract.inputRank());
        requireTensor(inputs, contract.itemInput(), "input", contract.inputType(), contract.inputRank());
        for (String name : inputs.keySet()) {
            if (!name.equals(contract.userInput()) && !name.equals(contract.itemInput())) {
                // Serving has nothing to feed an extra input; the run would fail on every request.
                throw new IllegalStateException("ONNX model declares unexpected input '" + name
                        + "'; serving can only populate " + contract.userInput() + " and " + contract.itemInput());
            }
        }
        // Extra outputs are harmless: only the contract output is ever read.
        requireTensor(handle.outputInfo(), contract.outputName(), "output", contract.outputType(), contract.outputRank());
    }

    private static void requireTensor(Map<String, NodeInfo> nodes, String name, String kind,
                                      TensorType expectedType, int expectedRank) {
        NodeInfo node = nodes.get(name);
        if (node == null) {
            throw new IllegalStateException("ONNX model has no " + kind + " named '" + name
                    + "'; found " + nodes.keySet());
        }
        ValueInfo info = node.getInfo();
        if (!(info instanceof TensorInfo tensor)) {
            throw new IllegalStateException("ONNX " + kind + " '" + name + "' is not a tensor: " + info);
        }
        OnnxJavaType expected = toOrt(expectedType);
        if (tensor.type != expected) {
            throw new IllegalStateException("ONNX " + kind + " '" + name + "' must be " + expectedType
                    + " (" + expected + "), got " + tensor.type);
        }
        int rank = tensor.getShape().length;
        if (rank != expectedRank) {
            throw new IllegalStateException("ONNX " + kind + " '" + name + "' must have rank "
                    + expectedRank + ", got rank " + rank);
        }
    }

    private static OnnxJavaType toOrt(TensorType type) {
        return switch (type) {
            case INT64 -> OnnxJavaType.INT64;
            case FLOAT -> OnnxJavaType.FLOAT;
        };
    }

    private void smokeTest(OnnxSessionHandle handle) throws OrtException {
        float[] scores = run(handle, new long[]{smokeInputs.userIndex()}, new long[]{smokeInputs.itemIndex()});
        if (scores.length != 1) {
            throw new IllegalStateException("ONNX smoke inference returned " + scores.length
                    + " scores for a single-row batch");
        }
        if (!Float.isFinite(scores[0])) {
            throw new IllegalStateException("ONNX smoke inference returned a non-finite score: " + scores[0]);
        }
    }

    private float[] run(OnnxSessionHandle handle, long[] users, long[] items) throws OrtException {
        runCount.incrementAndGet();
        runCounter.increment();
        return handle.run(contract.userInput(), contract.itemInput(), contract.outputName(), users, items);
    }

    public boolean isReady() {
        return ready;
    }

    /** Native runs so far, including the smoke inference. Public so InferenceLoadTest (another package) can assert on it. */
    public long runCount() {
        return runCount.get();
    }

    public double score(FeatureEncoder.EncodedFeatures features, long itemId) {
        float[] scores = scoreBatch(features, new long[]{itemId});
        return scores[0];
    }

    private float[] scoreBatch(FeatureEncoder.EncodedFeatures features, long[] itemIds) {
        OnnxSessionHandle current = session;
        if (!ready || current == null) {
            throw new IllegalStateException("ONNX session is not initialized or has been closed");
        }
        if (features == null || itemIds == null || itemIds.length == 0) {
            return new float[0];
        }
        long[] userArr = new long[itemIds.length];
        for (int i = 0; i < userArr.length; i++) {
            userArr[i] = features.getUserId();
        }
        float[] scores;
        try {
            scores = run(current, userArr, itemIds);
        } catch (OrtException e) {
            throw new RuntimeException("Failed to run ONNX inference", e);
        }
        if (scores.length != itemIds.length) {
            throw new IllegalStateException("ONNX output contract violation: submitted " + itemIds.length
                    + " items but received " + scores.length + " scores");
        }
        for (float score : scores) {
            if (!Float.isFinite(score)) {
                throw new IllegalStateException("ONNX output contract violation: non-finite score " + score);
            }
        }
        return scores;
    }

    public List<ScoredItem> scoreCandidates(
            FeatureEncoder.EncodedFeatures features,
            FeatureEncoder featureEncoder,
            Set<String> candidateItemIds,
            int k
    ) {
        if (candidateItemIds == null || candidateItemIds.isEmpty() || k <= 0) {
            return List.of();
        }

        List<String> itemIds = new ArrayList<>(candidateItemIds.size());
        long[] encodedItemIds = new long[candidateItemIds.size()];
        int count = 0;
        for (String itemId : candidateItemIds) {
            Long encodedItemId = featureEncoder.encodeItemId(itemId);
            if (encodedItemId == null) {
                continue;
            }
            itemIds.add(itemId);
            encodedItemIds[count++] = encodedItemId;
        }
        if (count == 0) {
            return List.of();
        }
        if (count < encodedItemIds.length) {
            long[] compact = new long[count];
            System.arraycopy(encodedItemIds, 0, compact, 0, count);
            encodedItemIds = compact;
        }

        float[] scores = scoreBatch(features, encodedItemIds);
        PriorityQueue<ScoredItem> best = ScoredItems.minHeap();
        for (int i = 0; i < scores.length; i++) {
            double score = scores[i];
            ScoredItems.keepTopK(best, new ScoredItem(itemIds.get(i), score), k);
        }

        return ScoredItems.descending(best);
    }

    /** Idempotent. Flips readiness first so a concurrent scorer fails fast instead of racing the native close. */
    public synchronized void close() throws OrtException {
        ready = false;
        OnnxSessionHandle current = session;
        session = null;
        if (current != null) {
            current.close();
        }
    }
}
