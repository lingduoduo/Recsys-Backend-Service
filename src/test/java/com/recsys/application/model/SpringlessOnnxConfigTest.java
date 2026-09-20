package com.recsys.application.model;

import com.recsys.config.ModelServingProperties;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two production constructors build ModelServingProperties themselves rather than taking the
 * Spring-bound bean. Before this test they returned the hard-coded field initializers, so a
 * deployment that set RECSYS_MODEL_ONNX_INTRA_OP_THREADS=4 got 4 through the Spring path and 1
 * through these — a disagreement invisible while the defaults happen to match, and visible
 * exactly when someone is trying to tune.
 *
 * <p>Source-level rather than behavioural: constructing either class for real loads ONNX
 * artifacts and opens Redis pools, which is a far heavier thing than the property under test.
 * What has to hold is that neither site calls the bare constructor.
 */
class SpringlessOnnxConfigTest {

    @Test
    void theEnvironmentFactoriesExistAndArePublic() throws Exception {
        Method onnxFactory = ModelServingProperties.Onnx.class.getMethod("fromEnvironment");
        Method propertiesFactory = ModelServingProperties.class.getMethod("fromEnvironment");

        assertThat(onnxFactory.getReturnType()).isEqualTo(ModelServingProperties.Onnx.class);
        assertThat(propertiesFactory.getReturnType()).isEqualTo(ModelServingProperties.class);
    }

    @Test
    void neitherSpringlessSiteStillCallsTheBareConstructor() throws Exception {
        String userTower = Files.readString(Path.of(
                "src/main/java/com/recsys/application/retrieval/UserTowerInferenceService.java"));
        String runtimeProvider = Files.readString(Path.of(
                "src/main/java/com/recsys/application/model/ModelRuntimeProvider.java"));

        assertThat(userTower)
                .as("UserTowerInferenceService's legacy constructor must not seat hard-coded ONNX defaults")
                .doesNotContain("new ModelServingProperties.Onnx()")
                .contains("ModelServingProperties.Onnx.fromEnvironment()");
        assertThat(runtimeProvider)
                .as("ModelRuntimeProvider's non-Spring constructor must not seat hard-coded ONNX defaults")
                .doesNotContain("new ModelServingProperties()")
                .contains("ModelServingProperties.fromEnvironment()");
    }

    @Test
    void environmentSourcedDefaultsStillMatchTheHardCodedOnes() {
        // Nothing is retuned by this change: with the variables unset both agree.
        ModelServingProperties fromEnv = ModelServingProperties.fromEnvironment(name -> null);
        ModelServingProperties hardCoded = new ModelServingProperties();

        assertThat(fromEnv.getOnnx().getIntraOpThreads()).isEqualTo(hardCoded.getOnnx().getIntraOpThreads());
        assertThat(fromEnv.getOnnx().getInterOpThreads()).isEqualTo(hardCoded.getOnnx().getInterOpThreads());
        assertThat(fromEnv.getOnnx().getExecutionMode()).isEqualTo(hardCoded.getOnnx().getExecutionMode());
    }
}
