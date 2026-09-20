package com.recsys.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.Locale;

/**
 * Runtime tuning for ONNX inference and parallel recommendation recall.
 */
@Validated
@ConfigurationProperties(prefix = "recsys.model")
public class ModelServingProperties {

    @Valid
    private final Onnx onnx = new Onnx();

    @Valid
    private final Recall recall = new Recall();

    public Onnx getOnnx() {
        return onnx;
    }

    public Recall getRecall() {
        return recall;
    }

    /**
     * A properties object for the code paths Spring did not build. Only the ONNX block is
     * environment-sourced; {@code recsys.model.recall.*} keeps its hard-coded defaults here,
     * deliberately -- see docs/superpowers/specs/2026-09-20-onnx-thread-config-unification-design.md.
     */
    public static ModelServingProperties fromEnvironment() {
        return fromEnvironment(System::getenv);
    }

    public static ModelServingProperties fromEnvironment(EnvVars.EnvReader env) {
        ModelServingProperties properties = new ModelServingProperties();
        properties.onnx.applyEnvironment(env);
        return properties;
    }

    public enum ExecutionMode {
        SEQUENTIAL,
        PARALLEL
    }

    public static class Onnx {

        public static final String INTRA_OP_THREADS_ENV = "RECSYS_MODEL_ONNX_INTRA_OP_THREADS";
        public static final String INTER_OP_THREADS_ENV = "RECSYS_MODEL_ONNX_INTER_OP_THREADS";
        public static final String EXECUTION_MODE_ENV = "RECSYS_MODEL_ONNX_EXECUTION_MODE";

        @Positive
        private int intraOpThreads = 1;

        @Positive
        private int interOpThreads = 1;

        @NotNull
        private ExecutionMode executionMode = ExecutionMode.SEQUENTIAL;

        public int getIntraOpThreads() {
            return intraOpThreads;
        }

        public void setIntraOpThreads(int intraOpThreads) {
            this.intraOpThreads = requirePositive(intraOpThreads, "intraOpThreads");
        }

        public int getInterOpThreads() {
            return interOpThreads;
        }

        public void setInterOpThreads(int interOpThreads) {
            this.interOpThreads = requirePositive(interOpThreads, "interOpThreads");
        }

        public ExecutionMode getExecutionMode() {
            return executionMode;
        }

        public void setExecutionMode(ExecutionMode executionMode) {
            if (executionMode == null) {
                throw new IllegalArgumentException("executionMode must not be null");
            }
            this.executionMode = executionMode;
        }

        /**
         * The same three settings {@code application.yml} exposes as {@code ${RECSYS_MODEL_ONNX_*}},
         * read directly. Spring is not the only thing that builds this class: the constructors at
         * {@code UserTowerInferenceService} and {@code ModelRuntimeProvider} that take no properties
         * object would otherwise serve the hard-coded field initializers and silently disagree with
         * a deployment that overrode the variables.
         *
         * <p>Parsing goes through {@link EnvVars}, which throws on an unparseable value, rather than
         * {@link EnvConfig}, which returns the default. Both paths must reject the same input: the
         * Spring path already fails context startup on a non-numeric or non-positive value, and a
         * variable that means one thing when Spring reads it and another when this does is the
         * defect, not the fix.
         */
        public static Onnx fromEnvironment() {
            return fromEnvironment(System::getenv);
        }

        public static Onnx fromEnvironment(EnvVars.EnvReader env) {
            Onnx onnx = new Onnx();
            onnx.applyEnvironment(env);
            return onnx;
        }

        /** Routes through the setters so {@code @Positive} validation covers env-sourced values too. */
        private void applyEnvironment(EnvVars.EnvReader env) {
            setIntraOpThreads(EnvVars.readInt(env, INTRA_OP_THREADS_ENV, intraOpThreads));
            setInterOpThreads(EnvVars.readInt(env, INTER_OP_THREADS_ENV, interOpThreads));
            setExecutionMode(readExecutionMode(env, executionMode));
        }

        private static ExecutionMode readExecutionMode(EnvVars.EnvReader env, ExecutionMode defaultMode) {
            String raw = env.get(EXECUTION_MODE_ENV);
            if (raw == null || raw.isBlank()) {
                return defaultMode;
            }
            try {
                return ExecutionMode.valueOf(raw.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("env var " + EXECUTION_MODE_ENV
                        + " is not a valid execution mode: " + raw);
            }
        }
    }

    public static class Recall {

        @Min(0)
        private int coreThreads = defaultCoreThreads();

        @Positive
        private int queueCapacity = 256;

        @Positive
        private long timeoutMs = 200;

        public int getCoreThreads() {
            return coreThreads;
        }

        public void setCoreThreads(int coreThreads) {
            if (coreThreads < 0) {
                throw new IllegalArgumentException("coreThreads must not be negative");
            }
            this.coreThreads = coreThreads == 0 ? defaultCoreThreads() : coreThreads;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = requirePositive(queueCapacity, "queueCapacity");
        }

        public long getTimeoutMs() {
            return timeoutMs;
        }

        public void setTimeoutMs(long timeoutMs) {
            this.timeoutMs = requirePositive(timeoutMs, "timeoutMs");
        }
    }

    private static int defaultCoreThreads() {
        return Math.max(1, Runtime.getRuntime().availableProcessors() * 2);
    }

    private static int requirePositive(int value, String propertyName) {
        if (value < 1) {
            throw new IllegalArgumentException(propertyName + " must be at least 1");
        }
        return value;
    }

    private static long requirePositive(long value, String propertyName) {
        if (value < 1) {
            throw new IllegalArgumentException(propertyName + " must be at least 1");
        }
        return value;
    }
}
