package com.demo.retrieval.evaluation;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/** Command-line comparison of uniform and greedy policies on a fixed MovieLens dataset. */
public final class MovieLensPolicyEvaluation {
    private static final String CSV_HEADER =
            "policy,episodes,mean_return,mean_steps,standard_error,ci95_low,ci95_high";

    private MovieLensPolicyEvaluation() {}

    public record Options(Path ratings, int episodes, int candidatePoolSize, int slateSize,
                          int minUserRatings, int minMovieRatings, double unratedReward,
                          double discount, long seed, int bootstrapSamples, Path output) {
        public static Options parse(String[] args) {
            Map<String, String> values = new HashMap<>();
            for (int index = 0; index < args.length; index += 2) {
                String option = args[index];
                if (!option.startsWith("--")) {
                    throw new IllegalArgumentException("Expected an option, got: " + option);
                }
                if (!known(option)) {
                    throw new IllegalArgumentException("Unknown option: " + option);
                }
                if (values.containsKey(option)) {
                    throw new IllegalArgumentException("Duplicate option: " + option);
                }
                if (index + 1 >= args.length || args[index + 1].startsWith("--")) {
                    throw new IllegalArgumentException(option + " requires a value");
                }
                values.put(option, args[index + 1]);
            }
            if (!values.containsKey("--ratings")) {
                throw new IllegalArgumentException("--ratings is required");
            }

            int episodes = integer(values, "--episodes", 5000);
            int pool = integer(values, "--candidate-pool-size", 100);
            int slate = integer(values, "--slate-size", 10);
            int minUsers = integer(values, "--min-user-ratings", 20);
            int minMovies = integer(values, "--min-movie-ratings", 10);
            int bootstrap = integer(values, "--bootstrap-samples", 1000);
            double unrated = decimal(values, "--unrated-reward", -1.0);
            double discount = decimal(values, "--discount", 0.99);
            long seed = longInteger(values, "--seed", 42L);

            positive(episodes, "--episodes");
            positive(pool, "--candidate-pool-size");
            positive(slate, "--slate-size");
            positive(bootstrap, "--bootstrap-samples");
            nonnegative(minUsers, "--min-user-ratings");
            nonnegative(minMovies, "--min-movie-ratings");
            if (!Double.isFinite(unrated)) {
                throw new IllegalArgumentException("--unrated-reward must be finite");
            }
            if (!Double.isFinite(discount) || discount < 0.0 || discount > 1.0) {
                throw new IllegalArgumentException("--discount must be in [0, 1]");
            }
            return new Options(Path.of(values.get("--ratings")), episodes, pool, slate,
                    minUsers, minMovies, unrated, discount, seed, bootstrap,
                    values.containsKey("--output") ? Path.of(values.get("--output")) : null);
        }

        private static boolean known(String option) {
            return switch (option) {
                case "--ratings", "--episodes", "--candidate-pool-size", "--slate-size",
                        "--min-user-ratings", "--min-movie-ratings", "--unrated-reward",
                        "--discount", "--seed", "--bootstrap-samples", "--output" -> true;
                default -> false;
            };
        }

        private static int integer(Map<String, String> values, String option, int fallback) {
            String value = values.get(option);
            if (value == null) return fallback;
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException("Invalid integer for " + option + ": " + value);
            }
        }

        private static long longInteger(Map<String, String> values, String option, long fallback) {
            String value = values.get(option);
            if (value == null) return fallback;
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException("Invalid integer for " + option + ": " + value);
            }
        }

        private static double decimal(Map<String, String> values, String option, double fallback) {
            String value = values.get(option);
            if (value == null) return fallback;
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException("Invalid number for " + option + ": " + value);
            }
        }

        private static void positive(int value, String option) {
            if (value <= 0) throw new IllegalArgumentException(option + " must be positive");
        }

        private static void nonnegative(int value, String option) {
            if (value < 0) throw new IllegalArgumentException(option + " must be nonnegative");
        }
    }

    public record Result(String policy, int episodes, double meanReturn, double meanSteps,
                         double standardError, double ci95Low, double ci95High) {}

    public static List<Result> evaluate(MovieLensDataset dataset,
                                        FiniteHorizonEnvironment environment,
                                        int episodes, double discount, long seed,
                                        int bootstrapSamples) {
        if (episodes <= 0) throw new IllegalArgumentException("Episodes must be positive");
        if (bootstrapSamples <= 0) {
            throw new IllegalArgumentException("Bootstrap samples must be positive");
        }
        List<NamedPolicy> policies = List.of(
                new NamedPolicy("uniform", EvaluationPolicy.uniform()),
                new NamedPolicy("greedy", EvaluationPolicy.greedy(dataset)));
        List<Result> results = new ArrayList<>(policies.size());
        for (NamedPolicy named : policies) {
            List<Double> returns = new ArrayList<>(episodes);
            long stepTotal = 0;
            for (int episode = 0; episode < episodes; episode++) {
                Random environmentRandom = new Random(derivedSeed(seed, episode, "environment"));
                Random policyRandom = new Random(derivedSeed(seed, episode,
                        named.name() + ":policy"));
                int userIndex = new Random(derivedSeed(seed, episode, "user"))
                        .nextInt(dataset.userIds().size());
                String userId = dataset.userIds().get(userIndex);
                FiniteHorizonEnvironment.Rollout rollout =
                        environment.rollout(userId, named.policy(), environmentRandom,
                                policyRandom, discount);
                returns.add(rollout.discountedReturn());
                stepTotal += rollout.steps();
            }
            double mean = returns.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
            double standardError = standardError(returns, mean);
            double[] bounds = bootstrapBounds(returns, bootstrapSamples,
                    derivedSeed(seed, bootstrapSamples, named.name() + ":bootstrap"));
            results.add(new Result(named.name(), episodes, mean, (double) stepTotal / episodes,
                    standardError, bounds[0], bounds[1]));
        }
        return List.copyOf(results);
    }

    public static double[] bootstrapBounds(List<Double> returns, int samples, long seed) {
        if (returns.isEmpty()) throw new IllegalArgumentException("Returns must not be empty");
        if (samples <= 0) throw new IllegalArgumentException("Bootstrap samples must be positive");
        Random random = new Random(seed);
        double[] means = new double[samples];
        for (int sample = 0; sample < samples; sample++) {
            double total = 0.0;
            for (int draw = 0; draw < returns.size(); draw++) {
                total += returns.get(random.nextInt(returns.size()));
            }
            means[sample] = total / returns.size();
        }
        Arrays.sort(means);
        return new double[] {percentile(means, 0.025), percentile(means, 0.975)};
    }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        try {
            Options options = Options.parse(args);
            if (!Files.isRegularFile(options.ratings()) || !Files.isReadable(options.ratings())) {
                throw new IllegalArgumentException("Ratings file is not readable: " + options.ratings());
            }
            MovieLensDataset dataset = MovieLensDataset.load(options.ratings(),
                    options.minUserRatings(), options.minMovieRatings());
            FiniteHorizonEnvironment environment = new FiniteHorizonEnvironment(dataset,
                    options.candidatePoolSize(), options.slateSize(), options.unratedReward());
            List<Result> results = evaluate(dataset, environment, options.episodes(),
                    options.discount(), options.seed(), options.bootstrapSamples());
            if (options.output() != null) writeCsv(results, options.output());
            printConsole(results, out);
            return 0;
        } catch (IllegalArgumentException | IOException error) {
            err.println("error: " + error.getMessage());
            return 2;
        }
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    private static void printConsole(List<Result> results, PrintStream out) {
        out.println("policy   episodes  mean_return  mean_steps  standard_error  ci95_low  ci95_high");
        results.forEach(result -> out.printf(Locale.ROOT,
                "%-8s %8d %12.6f %11.6f %15.6f %9.6f %10.6f%n",
                result.policy(), result.episodes(), result.meanReturn(), result.meanSteps(),
                result.standardError(), result.ci95Low(), result.ci95High()));
        out.println("Intervals quantify episode-sampling uncertainty for this fixed dataset;");
        out.println("they do not quantify uncertainty in the MovieLens dataset itself.");
    }

    private static void writeCsv(List<Result> results, Path output) throws IOException {
        try (var writer = Files.newBufferedWriter(output)) {
            writer.write(CSV_HEADER);
            writer.newLine();
            for (Result result : results) {
                writer.write(String.format(Locale.ROOT, "%s,%d,%.6f,%.6f,%.6f,%.6f,%.6f",
                        result.policy(), result.episodes(), result.meanReturn(), result.meanSteps(),
                        result.standardError(), result.ci95Low(), result.ci95High()));
                writer.newLine();
            }
        }
    }

    private static double standardError(List<Double> values, double mean) {
        if (values.size() == 1) return 0.0;
        double squaredDeviations = values.stream()
                .mapToDouble(value -> (value - mean) * (value - mean)).sum();
        double sampleVariance = squaredDeviations / (values.size() - 1);
        return Math.sqrt(sampleVariance) / Math.sqrt(values.size());
    }

    private static double percentile(double[] sorted, double probability) {
        double position = probability * (sorted.length - 1);
        int lower = (int) Math.floor(position);
        int upper = (int) Math.ceil(position);
        if (lower == upper) return sorted[lower];
        double fraction = position - lower;
        return sorted[lower] + fraction * (sorted[upper] - sorted[lower]);
    }

    private static long derivedSeed(long base, int episode, String purpose) {
        long value = base ^ 0x9E3779B97F4A7C15L;
        value = mix(value + episode * 0xBF58476D1CE4E5B9L);
        return mix(value ^ purpose.hashCode());
    }

    private static long mix(long value) {
        value = (value ^ (value >>> 30)) * 0xBF58476D1CE4E5B9L;
        value = (value ^ (value >>> 27)) * 0x94D049BB133111EBL;
        return value ^ (value >>> 31);
    }

    private record NamedPolicy(String name, EvaluationPolicy policy) {}
}
