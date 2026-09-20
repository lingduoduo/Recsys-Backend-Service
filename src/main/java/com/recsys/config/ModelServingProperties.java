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
     * A properties object for the code paths Spring did not build, covering both blocks:
     * {@code recsys.model.onnx.*} (PR #340) and {@code recsys.model.recall.*} (issue #342).
     */
    public static ModelServingProperties fromEnvironment() {
        return fromEnvironment(System::getenv);
    }

    public static ModelServingProperties fromEnvironment(EnvVars.EnvReader env) {
        ModelServingProperties properties = new ModelServingProperties();
        properties.onnx.applyEnvironment(env);
        properties.recall.applyEnvironment(env);
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
         * {@link EnvConfig}, which returns the default. The two paths are held to the same verdict
         * on every input — including a blank thread count, which Spring rejects and which
         * {@link #readThreadCount} therefore rejects too. A variable that means one thing when
         * Spring reads it and another when this does is the defect, not the fix.
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
            setIntraOpThreads(readBoundedInt(env, INTRA_OP_THREADS_ENV, intraOpThreads, 1));
            setInterOpThreads(readBoundedInt(env, INTER_OP_THREADS_ENV, interOpThreads, 1));
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
                        + " is not a valid execution mode: " + raw, e);
            }
        }
    }

    public static class Recall {

        public static final String CORE_THREADS_ENV = "RECSYS_MODEL_RECALL_CORE_THREADS";
        public static final String QUEUE_CAPACITY_ENV = "RECSYS_MODEL_RECALL_QUEUE_CAPACITY";
        public static final String TIMEOUT_MS_ENV = "RECSYS_MODEL_RECALL_TIMEOUT_MS";

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

        /**
         * The recall half of {@link ModelServingProperties#fromEnvironment}, closing the gap
         * issue #342 recorded: {@code ModelRuntimeProvider} reads {@link #getCoreThreads()} and
         * friends, so a construction path Spring did not build served hard-coded initializers
         * here exactly as it did for the ONNX block before PR #340.
         *
         * <p>The floors are per-property and measured, not guessed. Spring ACCEPTS
         * {@code core-threads=0} — it is the documented "use {@code 2 x availableProcessors}"
         * input, and {@link #setCoreThreads} expands it — while REJECTING
         * {@code queue-capacity=0} and {@code timeout-ms=0}. A single "at least 1" rule, which
         * is what the ONNX block uses, would therefore reject a valid input here.
         */
        public static Recall fromEnvironment() {
            return fromEnvironment(System::getenv);
        }

        public static Recall fromEnvironment(EnvVars.EnvReader env) {
            Recall recall = new Recall();
            recall.applyEnvironment(env);
            return recall;
        }

        private void applyEnvironment(EnvVars.EnvReader env) {
            // coreThreads is compared against the raw floor of 0 before the setter expands it,
            // so "0" stays the valid "use the computed default" input Spring accepts.
            setCoreThreads(readBoundedInt(env, CORE_THREADS_ENV, coreThreads, 0));
            setQueueCapacity(readBoundedInt(env, QUEUE_CAPACITY_ENV, queueCapacity, 1));
            setTimeoutMs(readBoundedLong(env, TIMEOUT_MS_ENV, timeoutMs, 1));
        }
    }

    private static int defaultCoreThreads() {
        return Math.max(1, Runtime.getRuntime().availableProcessors() * 2);
    }

    /**
     * A blank-but-present value is rejected rather than defaulted, because that is what the
     * Spring path does. {@code ${RECSYS_MODEL_ONNX_INTRA_OP_THREADS:1}} supplies its default only
     * when the variable is UNSET, so an empty value reaches a primitive setter and fails context
     * startup with "A null value cannot be assigned to a primitive type" — measured against an
     * {@code ApplicationContextRunner}, not assumed, and true of every numeric property in this
     * class. {@code Onnx.readExecutionMode} deliberately differs and accepts blank, because
     * Spring binds that enum as a nullable object and falls back there. The asymmetry is
     * Spring's; matching it is the whole point of these factories.
     *
     * <p>{@code min} is a parameter rather than a constant because the floors genuinely differ:
     * {@code Recall.coreThreads} accepts 0 as "use the computed default", where every ONNX thread
     * count and the remaining recall settings require at least 1.
     *
     * <p>Every failure names the variable. On the Spring path a {@code BindException} supplies
     * that context; on this path nothing does, and the stated cost of failing fast is a
     * crash-looping pod whose log line had better say which variable caused it.
     */
    private static int readBoundedInt(EnvVars.EnvReader env, String name, int defaultValue, int min) {
        String raw = requirePresentOrNull(env, name, defaultValue);
        if (raw == null) {
            return defaultValue;
        }
        int value = EnvVars.readInt(env, name, defaultValue);
        if (value < min) {
            throw new IllegalStateException("env var " + name + " must be at least " + min + ", got: " + raw);
        }
        return value;
    }

    private static long readBoundedLong(EnvVars.EnvReader env, String name, long defaultValue, long min) {
        String raw = requirePresentOrNull(env, name, defaultValue);
        if (raw == null) {
            return defaultValue;
        }
        long value = EnvVars.readLong(env, name, defaultValue);
        if (value < min) {
            throw new IllegalStateException("env var " + name + " must be at least " + min + ", got: " + raw);
        }
        return value;
    }

    /** Returns null when the variable is unset; throws when it is set but blank. */
    private static String requirePresentOrNull(EnvVars.EnvReader env, String name, Object defaultValue) {
        String raw = env.get(name);
        if (raw == null) {
            return null;
        }
        if (raw.isBlank()) {
            throw new IllegalStateException("env var " + name
                    + " is set but blank; unset it to use the default of " + defaultValue);
        }
        return raw;
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
