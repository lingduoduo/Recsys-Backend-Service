package com.recsys.application.model;

import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import com.recsys.config.ModelServingProperties;

/**
 * The single place ONNX Runtime session threading is configured.
 *
 * <p>It exists because there are two session-creation sites in the model service JVM — the
 * two-tower {@code OrtSessionHandle} and the {@code mlp_embedding} session in
 * {@code DeepLearningPredictionService} — and for most of this project's life only the first
 * one applied any of these settings. The second ran on ONNX Runtime's own default, which is
 * derived from the visible CPU count: measured on an 8-core host it opened an intra-op pool of
 * 4 (three extra native threads per session) where the configured sessions beside it opened
 * none. Those pools are native, so no JVM thread metric could report the difference and
 * nothing noticed.
 *
 * <p>One caveat for a reader trusting a green build: the {@link #toOrt} mapping below is
 * verifiable by nothing. {@code SessionOptions} has no getters, and SEQUENTIAL and PARALLEL
 * produce identical inference output, so swapping the two arms would pass every test in this
 * repository. Consolidating the mapping here does not make it covered — it only makes the one
 * uncovered copy easier to find.
 *
 * <p>Takes the {@code SessionOptions} rather than returning one on purpose. It holds native
 * memory and must be closed by whoever opened it; handing a fresh one back across a package
 * boundary is how that ownership gets lost.
 */
public final class OnnxSessionOptions {

    private OnnxSessionOptions() {
    }

    public static void apply(OrtSession.SessionOptions options, ModelServingProperties.Onnx onnx)
            throws OrtException {
        options.setIntraOpNumThreads(onnx.getIntraOpThreads());
        options.setInterOpNumThreads(onnx.getInterOpThreads());
        options.setExecutionMode(toOrt(onnx.getExecutionMode()));
    }

    private static OrtSession.SessionOptions.ExecutionMode toOrt(ModelServingProperties.ExecutionMode mode) {
        return switch (mode) {
            case SEQUENTIAL -> OrtSession.SessionOptions.ExecutionMode.SEQUENTIAL;
            case PARALLEL -> OrtSession.SessionOptions.ExecutionMode.PARALLEL;
        };
    }
}
