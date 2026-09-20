package com.recsys.application.retrieval;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import com.recsys.application.model.OnnxSessionOptions;
import com.recsys.config.ModelServingProperties;

import java.nio.LongBuffer;
import java.util.Map;
import java.util.Optional;

/** Production {@link OnnxSessionHandle} over a real {@link OrtSession}. */
final class OrtSessionHandle implements OnnxSessionHandle {

    private final OrtEnvironment environment;
    private final OrtSession session;

    private OrtSessionHandle(OrtEnvironment environment, OrtSession session) {
        this.environment = environment;
        this.session = session;
    }

    static OnnxSessionHandle open(byte[] modelBytes, ModelServingProperties.Onnx onnx) throws OrtException {
        // OrtEnvironment is a JVM-wide singleton managed by the ONNX Runtime native layer.
        // Never close it here — that would invalidate every other variant's session.
        OrtEnvironment environment = OrtEnvironment.getEnvironment();
        // SessionOptions holds native memory of its own; it is safe to close once the session
        // has been created, and it must be closed even when createSession throws.
        try (OrtSession.SessionOptions options = new OrtSession.SessionOptions()) {
            OnnxSessionOptions.apply(options, onnx);
            return new OrtSessionHandle(environment, environment.createSession(modelBytes, options));
        }
    }

    @Override
    public Map<String, NodeInfo> inputInfo() throws OrtException {
        return session.getInputInfo();
    }

    @Override
    public Map<String, NodeInfo> outputInfo() throws OrtException {
        return session.getOutputInfo();
    }

    @Override
    public float[] run(String userInput, String itemInput, String outputName, long[] users, long[] items)
            throws OrtException {
        try (OnnxTensor userTensor = OnnxTensor.createTensor(
                     environment, LongBuffer.wrap(users), new long[]{users.length});
             OnnxTensor itemTensor = OnnxTensor.createTensor(
                     environment, LongBuffer.wrap(items), new long[]{items.length})) {
            Map<String, OnnxTensor> inputs = Map.of(userInput, userTensor, itemInput, itemTensor);
            try (OrtSession.Result result = session.run(inputs)) {
                Optional<OnnxValue> output = result.get(outputName);
                if (output.isEmpty()) {
                    throw new IllegalStateException("ONNX run returned no output named '" + outputName + "'");
                }
                Object value = output.get().getValue();
                if (!(value instanceof float[] scores)) {
                    throw new IllegalStateException("ONNX output '" + outputName + "' is not a rank-1 FLOAT tensor: "
                            + (value == null ? "null" : value.getClass().getSimpleName()));
                }
                return scores;
            }
        }
    }

    @Override
    public void close() {
        try {
            session.close();
        } catch (OrtException | RuntimeException e) {
            // Best-effort: a session that fails to close is already unusable.
        }
    }
}
