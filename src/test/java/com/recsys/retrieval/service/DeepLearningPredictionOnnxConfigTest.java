package com.recsys.retrieval.service;

import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.recsys.config.ModelServingProperties;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The mlp_embedding session is the second of the two OrtSessions in this JVM, and until this
 * test existed it was the one that read no thread configuration at all — it took ONNX
 * Runtime's CPU-derived default while the two-tower sessions beside it ran pinned to one
 * thread, in a pod whose CPU limit is 2.
 *
 * <p>Asserting that from the outside is not possible: OrtSession.SessionOptions exposes no
 * getters. So the seam is the same one OrtSessionHandle already uses — the opener takes the
 * resolved Onnx as an argument, and the test reads it there.
 */
class DeepLearningPredictionOnnxConfigTest {

    /** Records every Onnx config handed to it and returns a mock session. */
    private static final class RecordingOpener implements DeepLearningPredictionService.SessionOpener {
        private final List<ModelServingProperties.Onnx> opened = new ArrayList<>();

        @Override
        public OrtSession open(byte[] modelBytes, ModelServingProperties.Onnx onnx) throws OrtException {
            opened.add(onnx);
            return mock(OrtSession.class);
        }
    }

    private static ModelServingProperties.Onnx onnx(int intra, int inter,
                                                    ModelServingProperties.ExecutionMode mode) {
        ModelServingProperties.Onnx o = new ModelServingProperties.Onnx();
        o.setIntraOpThreads(intra);
        o.setInterOpThreads(inter);
        o.setExecutionMode(mode);
        return o;
    }

    @Test
    void sessionIsOpenedWithTheConfiguredThreadSettings() {
        RecordingOpener opener = new RecordingOpener();
        ModelServingProperties.Onnx configured = onnx(3, 2, ModelServingProperties.ExecutionMode.PARALLEL);

        new DeepLearningPredictionService(new ObjectMapper(), configured, opener);

        assertThat(opener.opened).hasSize(1);
        assertThat(opener.opened.get(0).getIntraOpThreads()).isEqualTo(3);
        assertThat(opener.opened.get(0).getInterOpThreads()).isEqualTo(2);
        assertThat(opener.opened.get(0).getExecutionMode())
                .isEqualTo(ModelServingProperties.ExecutionMode.PARALLEL);
    }

    @Test
    void reloadReopensWithTheSameConfiguration() throws Exception {
        RecordingOpener opener = new RecordingOpener();
        ModelServingProperties.Onnx configured = onnx(3, 2, ModelServingProperties.ExecutionMode.PARALLEL);

        DeepLearningPredictionService service =
                new DeepLearningPredictionService(new ObjectMapper(), configured, opener);
        service.reload();

        assertThat(opener.opened).hasSize(2);
        assertThat(opener.opened.get(1).getIntraOpThreads()).isEqualTo(3);
        assertThat(opener.opened.get(1).getExecutionMode())
                .isEqualTo(ModelServingProperties.ExecutionMode.PARALLEL);
    }

    @Test
    void springConstructorUsesThePropertiesObject() {
        RecordingOpener opener = new RecordingOpener();
        ModelServingProperties properties = new ModelServingProperties();
        properties.getOnnx().setIntraOpThreads(5);

        new DeepLearningPredictionService(new ObjectMapper(), properties.getOnnx(), opener);

        assertThat(opener.opened.get(0).getIntraOpThreads()).isEqualTo(5);
    }

    @Test
    void defaultsMatchTheTwoTowerSessionsRatherThanOnnxRuntimeDefaults() {
        RecordingOpener opener = new RecordingOpener();

        new DeepLearningPredictionService(
                new ObjectMapper(), ModelServingProperties.Onnx.fromEnvironment(name -> null), opener);

        assertThat(opener.opened.get(0).getIntraOpThreads()).isEqualTo(1);
        assertThat(opener.opened.get(0).getInterOpThreads()).isEqualTo(1);
        assertThat(opener.opened.get(0).getExecutionMode())
                .isEqualTo(ModelServingProperties.ExecutionMode.SEQUENTIAL);
    }
}
