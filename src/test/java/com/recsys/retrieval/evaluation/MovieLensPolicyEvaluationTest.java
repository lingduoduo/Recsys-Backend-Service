package com.demo.retrieval.evaluation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MovieLensPolicyEvaluationTest {
    @TempDir Path tempDir;

    @Test
    void evaluationIsDeterministicOrderedAndUsesZeroErrorForOneEpisode() throws Exception {
        MovieLensDataset data = dataset();
        FiniteHorizonEnvironment environment = new FiniteHorizonEnvironment(data, 3, 2, -1.0);

        List<MovieLensPolicyEvaluation.Result> first =
                MovieLensPolicyEvaluation.evaluate(data, environment, 20, 0.9, 17, 100);
        List<MovieLensPolicyEvaluation.Result> second =
                MovieLensPolicyEvaluation.evaluate(data, environment, 20, 0.9, 17, 100);
        List<MovieLensPolicyEvaluation.Result> single =
                MovieLensPolicyEvaluation.evaluate(data, environment, 1, 0.9, 17, 20);

        assertThat(first).isEqualTo(second);
        assertThat(first).extracting(MovieLensPolicyEvaluation.Result::policy)
                .containsExactly("uniform", "greedy");
        assertThat(single).allSatisfy(result -> assertThat(result.standardError()).isZero());
    }

    @Test
    void bootstrapBoundsAreDeterministicAndResampleRawReturns() {
        double[] first = MovieLensPolicyEvaluation.bootstrapBounds(
                List.of(-2.0, 0.0, 4.0, 8.0), 200, 31);
        double[] second = MovieLensPolicyEvaluation.bootstrapBounds(
                List.of(-2.0, 0.0, 4.0, 8.0), 200, 31);

        assertThat(first).containsExactly(second);
        assertThat(first).containsExactly(-0.5124999999999997, 6.0);
    }

    @Test
    void bootstrapAndPriorEvaluationCannotPerturbLaterPolicyResults() throws Exception {
        MovieLensDataset data = dataset();
        FiniteHorizonEnvironment environment = new FiniteHorizonEnvironment(data, 3, 2, -1.0);
        List<MovieLensPolicyEvaluation.Result> baseline =
                MovieLensPolicyEvaluation.evaluate(data, environment, 20, 0.9, 17, 100);

        MovieLensPolicyEvaluation.evaluate(data, environment, 7, 0.5, 999, 13);
        MovieLensPolicyEvaluation.bootstrapBounds(List.of(-100.0, 100.0), 1000, 8);

        assertThat(MovieLensPolicyEvaluation.evaluate(data, environment, 20, 0.9, 17, 100))
                .isEqualTo(baseline);
    }

    @Test
    void optionsUseDocumentedDefaults() {
        MovieLensPolicyEvaluation.Options options =
                MovieLensPolicyEvaluation.Options.parse(new String[] {"--ratings", "ratings.csv"});

        assertThat(options.ratings()).isEqualTo(Path.of("ratings.csv"));
        assertThat(options.episodes()).isEqualTo(5000);
        assertThat(options.candidatePoolSize()).isEqualTo(100);
        assertThat(options.slateSize()).isEqualTo(10);
        assertThat(options.minUserRatings()).isEqualTo(20);
        assertThat(options.minMovieRatings()).isEqualTo(10);
        assertThat(options.unratedReward()).isEqualTo(-1.0);
        assertThat(options.discount()).isEqualTo(0.99);
        assertThat(options.seed()).isEqualTo(42L);
        assertThat(options.bootstrapSamples()).isEqualTo(1000);
        assertThat(options.output()).isNull();
    }

    @Test
    void optionsRejectMissingDuplicateUnknownAndInvalidValues() {
        assertInvalid(new String[] {}, "--ratings is required");
        assertInvalid(new String[] {"--ratings"}, "--ratings requires a value");
        assertInvalid(new String[] {"--ratings", "a", "--ratings", "b"}, "Duplicate option: --ratings");
        assertInvalid(new String[] {"--ratings", "a", "--wat", "1"}, "Unknown option: --wat");
        assertInvalid(new String[] {"ratings.csv"}, "Expected an option, got: ratings.csv");
        assertInvalid(flag("--episodes", "0"), "--episodes must be positive");
        assertInvalid(flag("--candidate-pool-size", "0"), "--candidate-pool-size must be positive");
        assertInvalid(flag("--slate-size", "-1"), "--slate-size must be positive");
        assertInvalid(flag("--min-user-ratings", "-1"), "--min-user-ratings must be nonnegative");
        assertInvalid(flag("--min-movie-ratings", "-1"), "--min-movie-ratings must be nonnegative");
        assertInvalid(flag("--bootstrap-samples", "0"), "--bootstrap-samples must be positive");
        assertInvalid(flag("--discount", "-0.1"), "--discount must be in [0, 1]");
        assertInvalid(flag("--discount", "1.1"), "--discount must be in [0, 1]");
        assertInvalid(flag("--discount", "NaN"), "--discount must be in [0, 1]");
        assertInvalid(flag("--unrated-reward", "Infinity"), "--unrated-reward must be finite");
        assertInvalid(flag("--episodes", "abc"), "Invalid integer for --episodes: abc");
        assertInvalid(flag("--seed", "abc"), "Invalid integer for --seed: abc");
        assertInvalid(flag("--discount", "abc"), "Invalid number for --discount: abc");
        assertInvalid(flag("--unrated-reward", "abc"),
                "Invalid number for --unrated-reward: abc");
    }

    @Test
    void optionsAcceptInclusiveDiscountBoundaries() {
        assertThat(MovieLensPolicyEvaluation.Options.parse(flag("--discount", "0")).discount())
                .isZero();
        assertThat(MovieLensPolicyEvaluation.Options.parse(flag("--discount", "1")).discount())
                .isEqualTo(1.0);
    }

    @Test
    void runReportsErrorsWithoutExiting() {
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int missing = MovieLensPolicyEvaluation.run(new String[] {}, silent(), stream(err));
        assertThat(missing).isEqualTo(2);
        assertThat(err.toString(StandardCharsets.UTF_8)).isEqualTo("error: --ratings is required\n");

        err.reset();
        int unreadable = MovieLensPolicyEvaluation.run(
                new String[] {"--ratings", tempDir.resolve("missing.csv").toString()},
                silent(), stream(err));
        assertThat(unreadable).isEqualTo(2);
        assertThat(err.toString(StandardCharsets.UTF_8)).contains("error: Ratings file is not readable:");
    }

    @Test
    void validRunPrintsDisclaimerAndWritesExactCsv() throws Exception {
        Path ratings = datasetPath();
        Path output = tempDir.resolve("report.csv");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int status = MovieLensPolicyEvaluation.run(new String[] {
                "--ratings", ratings.toString(), "--episodes", "4",
                "--candidate-pool-size", "3", "--slate-size", "2",
                "--min-user-ratings", "0", "--min-movie-ratings", "0",
                "--bootstrap-samples", "20", "--output", output.toString()
        }, stream(out), stream(err));

        assertThat(status).isZero();
        assertThat(err.toString(StandardCharsets.UTF_8)).isEmpty();
        assertThat(out.toString(StandardCharsets.UTF_8))
                .contains("uniform", "greedy")
                .contains("Intervals quantify episode-sampling uncertainty for this fixed dataset;")
                .contains("they do not quantify uncertainty in the MovieLens dataset itself.");
        List<String> csv = Files.readAllLines(output);
        assertThat(csv.get(0)).isEqualTo(
                "policy,episodes,mean_return,mean_steps,standard_error,ci95_low,ci95_high");
        assertThat(csv).hasSize(3);
        assertThat(csv.get(1)).startsWith("uniform,4,");
        assertThat(csv.get(2)).startsWith("greedy,4,");
        assertThat(csv.subList(1, 3)).allSatisfy(line -> assertThat(line.split(",", -1)).hasSize(7));
    }

    @Test
    void validRunAcceptsRepositoryShapedOpaqueIdentifiers() throws Exception {
        Path ratings = tempDir.resolve("repository-ratings.csv");
        Files.writeString(ratings, """
                userId,movieId,rating,timestamp
                user_1,item_1,5,0
                user_1,item_2,4,0
                user_2,item_1,1,0
                user_2,item_2,5,0
                """);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int status = MovieLensPolicyEvaluation.run(new String[] {
                "--ratings", ratings.toString(), "--episodes", "20",
                "--candidate-pool-size", "2", "--slate-size", "2",
                "--min-user-ratings", "1", "--min-movie-ratings", "1",
                "--bootstrap-samples", "20"
        }, stream(out), stream(err));

        assertThat(status).isZero();
        assertThat(err.toString(StandardCharsets.UTF_8)).isEmpty();
        assertThat(out.toString(StandardCharsets.UTF_8)).contains("uniform", "greedy");
    }

    @Test
    void filteringEmptyUserPreventsSeedDependentUnknownUserFailure() throws Exception {
        Path ratings = tempDir.resolve("mixed-filter.csv");
        Files.writeString(ratings, """
                userId,movieId,rating,timestamp
                empty_after_movie_filter,rare,5,0
                retained,popular_a,4,0
                retained,popular_b,3,0
                also_retained,popular_a,2,0
                also_retained,popular_b,1,0
                """);

        for (long seed = 0; seed < 20; seed++) {
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            int status = MovieLensPolicyEvaluation.run(new String[] {
                    "--ratings", ratings.toString(), "--episodes", "5",
                    "--candidate-pool-size", "2", "--slate-size", "1",
                    "--min-user-ratings", "0", "--min-movie-ratings", "2",
                    "--bootstrap-samples", "10", "--seed", Long.toString(seed)
            }, silent(), stream(err));

            assertThat(status).as("seed %s error: %s", seed, err).isZero();
        }
    }

    @Test
    void outputFailurePrintsNoResultRowsOrDisclaimer() throws Exception {
        Path outputDirectory = Files.createDirectory(tempDir.resolve("not-a-csv"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int status = MovieLensPolicyEvaluation.run(new String[] {
                "--ratings", datasetPath().toString(), "--episodes", "2",
                "--candidate-pool-size", "2", "--slate-size", "1",
                "--min-user-ratings", "0", "--min-movie-ratings", "0",
                "--bootstrap-samples", "10", "--output", outputDirectory.toString()
        }, stream(out), stream(err));

        assertThat(status).isEqualTo(2);
        assertThat(out.toString(StandardCharsets.UTF_8)).isEmpty();
        assertThat(err.toString(StandardCharsets.UTF_8)).startsWith("error: ");
    }

    private void assertInvalid(String[] arguments, String message) {
        assertThatThrownBy(() -> MovieLensPolicyEvaluation.Options.parse(arguments))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

    private String[] flag(String name, String value) {
        return new String[] {"--ratings", "ratings.csv", name, value};
    }

    private MovieLensDataset dataset() throws Exception {
        return MovieLensDataset.load(datasetPath(), 0, 0);
    }

    private Path datasetPath() throws Exception {
        Path path = tempDir.resolve("ratings.csv");
        Files.writeString(path, """
                userId,movieId,rating,timestamp
                1,10,5,0
                1,20,4,0
                2,10,1,0
                2,30,5,0
                3,20,2,0
                3,30,4,0
                """);
        return path;
    }

    private PrintStream silent() {
        return stream(new ByteArrayOutputStream());
    }

    private PrintStream stream(ByteArrayOutputStream bytes) {
        return new PrintStream(bytes, true, StandardCharsets.UTF_8);
    }
}
