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

    /**
     * Observes whether the thread settings were read at all. The RecordingOpener above replaces
     * the opener wholesale, so it can only prove the config was *resolved* and handed over — it
     * never executes openOrtSession, where the config is actually applied. Deleting the
     * OnnxSessionOptions.apply call leaves every test that uses RecordingOpener green (measured).
     * SessionOptions exposes no getters, so a read of the config by the real opener is the only
     * observable proxy for "these settings reached the session".
     */
    private static final class SpyOnnx extends ModelServingProperties.Onnx {
        private final List<String> read = new ArrayList<>();

        @Override
        public int getIntraOpThreads() {
            read.add("intraOpThreads");
            return super.getIntraOpThreads();
        }

        @Override
        public int getInterOpThreads() {
            read.add("interOpThreads");
            return super.getInterOpThreads();
        }

        @Override
        public ModelServingProperties.ExecutionMode getExecutionMode() {
            read.add("executionMode");
            return super.getExecutionMode();
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

    /**
     * The one test here that runs the REAL opener against real ONNX Runtime. Everything else in
     * this class stubs the opener out, which is exactly the blind spot that let an unconfigured
     * session ship: passing a null opener is what makes openOrtSession run.
     */
    @Test
    void theRealOpenerAppliesEveryThreadSettingToTheSession() throws Exception {
        SpyOnnx spy = new SpyOnnx();

        DeepLearningPredictionService service =
                new DeepLearningPredictionService(new ObjectMapper(), spy, null);

        try {
            assertThat(spy.read)
                    .as("openOrtSession must apply all three settings to the SessionOptions")
                    .contains("intraOpThreads", "interOpThreads", "executionMode");
        } finally {
            service.close();
        }
    }
}
