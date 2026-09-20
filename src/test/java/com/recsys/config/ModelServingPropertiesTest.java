package com.recsys.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.recsys.config.ModelServingProperties.ExecutionMode.PARALLEL;
import static com.recsys.config.ModelServingProperties.ExecutionMode.SEQUENTIAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ModelServingPropertiesTest {

    @Test
    void defaultsAreConservative() {
        ModelServingProperties p = new ModelServingProperties();

        assertThat(p.getOnnx().getIntraOpThreads()).isEqualTo(1);
        assertThat(p.getOnnx().getInterOpThreads()).isEqualTo(1);
        assertThat(p.getOnnx().getExecutionMode()).isEqualTo(SEQUENTIAL);
        assertThat(p.getRecall().getQueueCapacity()).isEqualTo(256);
        assertThat(p.getRecall().getTimeoutMs()).isEqualTo(200);
    }

    @Test
    void rejectsNonPositiveThreadAndQueueValues() {
        ModelServingProperties p = new ModelServingProperties();

        assertThatThrownBy(() -> p.getOnnx().setIntraOpThreads(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> p.getRecall().setQueueCapacity(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readsOnnxSettingsFromTheEnvironment() {
        ModelServingProperties.Onnx onnx = ModelServingProperties.Onnx.fromEnvironment(Map.of(
                "RECSYS_MODEL_ONNX_INTRA_OP_THREADS", "3",
                "RECSYS_MODEL_ONNX_INTER_OP_THREADS", "4",
                "RECSYS_MODEL_ONNX_EXECUTION_MODE", "PARALLEL")::get);

        assertThat(onnx.getIntraOpThreads()).isEqualTo(3);
        assertThat(onnx.getInterOpThreads()).isEqualTo(4);
        assertThat(onnx.getExecutionMode()).isEqualTo(PARALLEL);
    }

    @Test
    void environmentFallbackMatchesTheHardCodedDefaults() {
        ModelServingProperties.Onnx fromEnv = ModelServingProperties.Onnx.fromEnvironment(name -> null);
        ModelServingProperties.Onnx hardCoded = new ModelServingProperties.Onnx();

        assertThat(fromEnv.getIntraOpThreads()).isEqualTo(hardCoded.getIntraOpThreads());
        assertThat(fromEnv.getInterOpThreads()).isEqualTo(hardCoded.getInterOpThreads());
        assertThat(fromEnv.getExecutionMode()).isEqualTo(hardCoded.getExecutionMode());
    }

    // Measured against an ApplicationContextRunner bound to recsys.model.onnx.*: Spring FAILS on a
    // blank thread count ("A null value cannot be assigned to a primitive type") because the
    // ${...:1} default applies only when the variable is UNSET, and ACCEPTS a blank execution mode
    // because it binds the enum as a nullable object. The env path has to reproduce both verdicts,
    // asymmetric as they look, or the same value means two different things depending on the reader.
    @Test
    void aBlankThreadCountIsRejectedBecauseSpringRejectsIt() {
        assertThatThrownBy(() -> ModelServingProperties.Onnx.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_INTRA_OP_THREADS", "  ")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_ONNX_INTRA_OP_THREADS")
                .hasMessageContaining("blank");

        assertThatThrownBy(() -> ModelServingProperties.Onnx.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_INTER_OP_THREADS", "")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_ONNX_INTER_OP_THREADS");
    }

    @Test
    void aBlankExecutionModeFallsBackBecauseSpringAcceptsIt() {
        ModelServingProperties.Onnx onnx = ModelServingProperties.Onnx.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_EXECUTION_MODE", "  ")::get);

        assertThat(onnx.getExecutionMode()).isEqualTo(SEQUENTIAL);
        assertThat(onnx.getIntraOpThreads()).isEqualTo(1);
    }

    @Test
    void executionModeIsCaseInsensitiveAndTrimmed() {
        ModelServingProperties.Onnx onnx = ModelServingProperties.Onnx.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_EXECUTION_MODE", " parallel ")::get);

        assertThat(onnx.getExecutionMode()).isEqualTo(PARALLEL);
    }

    // The Spring path fails context startup on each of these: ${...} bound to a @Positive
    // setter rejects 0 and -1, and relaxed binding rejects an unknown enum constant. The env
    // path has to fail too, or the same variable means two different things depending on who
    // read it -- which is the whole defect this change exists to close.
    @Test
    void invalidEnvironmentValuesFailFastRatherThanSilentlyDefaulting() {
        assertThatThrownBy(() -> ModelServingProperties.Onnx.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_INTRA_OP_THREADS", "abc")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_ONNX_INTRA_OP_THREADS");

        // Every env failure names the variable. The setter's own message ("intraOpThreads must be
        // at least 1") does not, and on this path there is no BindException to supply it -- the
        // stated cost of failing fast is a crash-looping pod, whose log line must say which
        // variable caused it.
        assertThatThrownBy(() -> ModelServingProperties.Onnx.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_INTRA_OP_THREADS", "0")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_ONNX_INTRA_OP_THREADS");

        assertThatThrownBy(() -> ModelServingProperties.Onnx.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_INTER_OP_THREADS", "-1")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_ONNX_INTER_OP_THREADS");

        assertThatThrownBy(() -> ModelServingProperties.Onnx.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_EXECUTION_MODE", "TURBO")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_ONNX_EXECUTION_MODE");
    }

    @Test
    void wholePropertiesFactoryCarriesBothBlocksFromTheEnvironment() {
        ModelServingProperties properties = ModelServingProperties.fromEnvironment(Map.of(
                "RECSYS_MODEL_ONNX_INTRA_OP_THREADS", "6",
                "RECSYS_MODEL_RECALL_QUEUE_CAPACITY", "512")::get);

        assertThat(properties.getOnnx().getIntraOpThreads()).isEqualTo(6);
        assertThat(properties.getRecall().getQueueCapacity()).isEqualTo(512);
    }

    @Test
    void readsRecallSettingsFromTheEnvironment() {
        ModelServingProperties.Recall recall = ModelServingProperties.Recall.fromEnvironment(Map.of(
                "RECSYS_MODEL_RECALL_CORE_THREADS", "5",
                "RECSYS_MODEL_RECALL_QUEUE_CAPACITY", "512",
                "RECSYS_MODEL_RECALL_TIMEOUT_MS", "350")::get);

        assertThat(recall.getCoreThreads()).isEqualTo(5);
        assertThat(recall.getQueueCapacity()).isEqualTo(512);
        assertThat(recall.getTimeoutMs()).isEqualTo(350);
    }

    @Test
    void recallEnvironmentFallbackMatchesTheHardCodedDefaults() {
        ModelServingProperties.Recall fromEnv = ModelServingProperties.Recall.fromEnvironment(name -> null);
        ModelServingProperties.Recall hardCoded = new ModelServingProperties.Recall();

        assertThat(fromEnv.getCoreThreads()).isEqualTo(hardCoded.getCoreThreads());
        assertThat(fromEnv.getQueueCapacity()).isEqualTo(hardCoded.getQueueCapacity());
        assertThat(fromEnv.getTimeoutMs()).isEqualTo(hardCoded.getTimeoutMs());
    }

    // Measured against an ApplicationContextRunner: Spring accepts core-threads=0 and resolves it
    // to 2 x availableProcessors (the documented "use the default" input), but REJECTS
    // queue-capacity=0 and timeout-ms=0. So the floor differs per property and cannot be the
    // single "at least 1" rule the ONNX block uses.
    @Test
    void zeroCoreThreadsMeansTwicetheProcessorCountJustAsItDoesUnderSpring() {
        ModelServingProperties.Recall recall = ModelServingProperties.Recall.fromEnvironment(
                Map.of("RECSYS_MODEL_RECALL_CORE_THREADS", "0")::get);

        assertThat(recall.getCoreThreads())
                .isEqualTo(Math.max(1, Runtime.getRuntime().availableProcessors() * 2));
    }

    @Test
    void invalidRecallEnvironmentValuesFailFastAndNameTheVariable() {
        assertThatThrownBy(() -> ModelServingProperties.Recall.fromEnvironment(
                Map.of("RECSYS_MODEL_RECALL_CORE_THREADS", "-1")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_RECALL_CORE_THREADS");

        assertThatThrownBy(() -> ModelServingProperties.Recall.fromEnvironment(
                Map.of("RECSYS_MODEL_RECALL_QUEUE_CAPACITY", "0")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_RECALL_QUEUE_CAPACITY");

        assertThatThrownBy(() -> ModelServingProperties.Recall.fromEnvironment(
                Map.of("RECSYS_MODEL_RECALL_TIMEOUT_MS", "0")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_RECALL_TIMEOUT_MS");

        assertThatThrownBy(() -> ModelServingProperties.Recall.fromEnvironment(
                Map.of("RECSYS_MODEL_RECALL_TIMEOUT_MS", "abc")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_RECALL_TIMEOUT_MS");
    }

    // Spring fails on a blank value for all three -- they are primitive-typed, and the ${...:N}
    // default applies only when the variable is UNSET. Same verdict required here.
    @Test
    void blankRecallValuesAreRejectedBecauseSpringRejectsThem() {
        assertThatThrownBy(() -> ModelServingProperties.Recall.fromEnvironment(
                Map.of("RECSYS_MODEL_RECALL_CORE_THREADS", "  ")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_RECALL_CORE_THREADS")
                .hasMessageContaining("blank");

        assertThatThrownBy(() -> ModelServingProperties.Recall.fromEnvironment(
                Map.of("RECSYS_MODEL_RECALL_TIMEOUT_MS", "")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_RECALL_TIMEOUT_MS");
    }
}
