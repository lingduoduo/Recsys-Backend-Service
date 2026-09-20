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
    void wholePropertiesFactoryCarriesTheOnnxEnvironment() {
        ModelServingProperties properties = ModelServingProperties.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_INTRA_OP_THREADS", "6")::get);

        assertThat(properties.getOnnx().getIntraOpThreads()).isEqualTo(6);
        // Recall is deliberately NOT environment-sourced here -- see the spec's non-goals.
        assertThat(properties.getRecall().getQueueCapacity()).isEqualTo(256);
    }
}
