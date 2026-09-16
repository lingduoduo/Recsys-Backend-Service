package com.demo.retrieval.evaluation;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Immutable ratings retained from a MovieLens CSV after bipartite fixed-point filtering. */
public final class MovieLensDataset {
    private static final double PRIOR_STRENGTH = 20.0;

    private final Map<String, Map<String, Double>> ratingsByUser;
    private final List<String> userIds;
    private final List<String> movieIds;
    private final List<String> movieIdsByPopularity;
    private final Map<String, Integer> movieCounts;
    private final Map<String, Double> movieSums;
    private final double globalMean;

    private MovieLensDataset(Map<String, Map<String, Double>> ratings) {
        Map<String, Map<String, Double>> immutableRatings = new LinkedHashMap<>();
        ratings.forEach((userId, userRatings) ->
                immutableRatings.put(userId,
                        Collections.unmodifiableMap(new LinkedHashMap<>(userRatings))));
        this.ratingsByUser = Collections.unmodifiableMap(immutableRatings);
        this.userIds = List.copyOf(ratingsByUser.keySet());

        TreeMap<String, Integer> counts = new TreeMap<>();
        TreeMap<String, Double> sums = new TreeMap<>();
        ratingsByUser.values().forEach(userRatings -> userRatings.forEach((movieId, rating) -> {
            counts.merge(movieId, 1, Integer::sum);
            sums.merge(movieId, rating, Double::sum);
        }));
        this.movieIds = List.copyOf(counts.keySet());
        this.movieCounts = Collections.unmodifiableMap(new LinkedHashMap<>(counts));
        this.movieSums = Collections.unmodifiableMap(new LinkedHashMap<>(sums));
        this.movieIdsByPopularity = movieIds.stream()
                .sorted(Comparator
                        .<String>comparingInt(movieId -> movieCounts.get(movieId))
                        .reversed()
                        .thenComparing(Comparator.naturalOrder()))
                .toList();
        double total = sums.values().stream().mapToDouble(Double::doubleValue).sum();
        int count = counts.values().stream().mapToInt(Integer::intValue).sum();
        this.globalMean = total / count;
    }

    public static MovieLensDataset load(Path file, int minUserRatings, int minMovieRatings)
            throws IOException {
        if (minUserRatings < 0 || minMovieRatings < 0) {
            throw new IllegalArgumentException("Rating thresholds must be nonnegative");
        }

        TreeMap<String, Map<String, Double>> ratings = new TreeMap<>();
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String header = reader.readLine();
            if (header == null || !hasExpectedHeader(header)) {
                throw new IllegalArgumentException("Expected header prefix userId,movieId,rating in " + file);
            }

            String row;
            int lineNumber = 1;
            while ((row = reader.readLine()) != null) {
                lineNumber++;
                String[] fields = row.split(",", -1);
                if (fields.length < 3) {
                    throw malformed(file, lineNumber, "expected at least three fields", null);
                }
                try {
                    String userId = identifier(fields[0], file, lineNumber, "user ID");
                    String movieId = identifier(fields[1], file, lineNumber, "movie ID");
                    double rating = Double.parseDouble(fields[2]);
                    if (!Double.isFinite(rating)) {
                        throw malformed(file, lineNumber, "rating must be finite", null);
                    }
                    Map<String, Double> userRatings =
                            ratings.computeIfAbsent(userId, ignored -> new TreeMap<>());
                    if (userRatings.putIfAbsent(movieId, rating) != null) {
                        throw malformed(file, lineNumber, "duplicate user/movie pair", null);
                    }
                } catch (NumberFormatException error) {
                    throw malformed(file, lineNumber, "invalid numeric field", error);
                }
            }
        }

        filterUntilStable(ratings, minUserRatings, minMovieRatings);
        if (ratings.values().stream().allMatch(Map::isEmpty)) {
            throw new IllegalArgumentException("No ratings remain after filtering " + file);
        }
        return new MovieLensDataset(ratings);
    }

    private static boolean hasExpectedHeader(String header) {
        String[] fields = header.split(",", -1);
        return fields.length >= 3
                && fields[0].equals("userId")
                && fields[1].equals("movieId")
                && fields[2].equals("rating");
    }

    private static String identifier(String value, Path file, int lineNumber, String field) {
        if (value.isBlank()) {
            throw malformed(file, lineNumber, field + " must be nonblank", null);
        }
        return value;
    }

    private static void filterUntilStable(Map<String, Map<String, Double>> ratings,
                                          int minUserRatings,
                                          int minMovieRatings) {
        boolean changed;
        do {
            Map<String, Long> counts = ratings.values().stream()
                    .flatMap(userRatings -> userRatings.keySet().stream())
                    .collect(Collectors.groupingBy(movieId -> movieId, Collectors.counting()));
            Set<String> removedMovies = counts.entrySet().stream()
                    .filter(entry -> entry.getValue() < minMovieRatings)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            int beforeUsers = ratings.size();
            int beforeRatings = ratings.values().stream().mapToInt(Map::size).sum();
            ratings.values().forEach(userRatings ->
                    removedMovies.forEach(userRatings::remove));
            ratings.entrySet().removeIf(entry -> entry.getValue().isEmpty()
                    || entry.getValue().size() < minUserRatings);
            int afterRatings = ratings.values().stream().mapToInt(Map::size).sum();
            changed = beforeUsers != ratings.size() || beforeRatings != afterRatings;
        } while (changed);
    }

    private static IllegalArgumentException malformed(Path file, int lineNumber,
                                                       String detail, Throwable cause) {
        String message = "Malformed ratings row in " + file + " at line " + lineNumber + ": " + detail;
        return cause == null ? new IllegalArgumentException(message)
                : new IllegalArgumentException(message, cause);
    }

    public List<String> userIds() {
        return userIds;
    }

    public List<String> movieIds() {
        return movieIds;
    }

    public List<String> movieIdsByPopularity() {
        return movieIdsByPopularity;
    }

    public Map<String, Double> ratingsFor(String userId) {
        return ratingsByUser.getOrDefault(userId, Map.of());
    }

    public double rating(String userId, String movieId) {
        return ratingsFor(userId).get(movieId);
    }

    public Map<String, Integer> movieCounts() {
        return movieCounts;
    }

    public double globalMean() {
        return globalMean;
    }

    public double scoreExcludingUser(String userId, String movieId) {
        double sum = movieSums.get(movieId);
        int count = movieCounts.get(movieId);
        Double heldOut = ratingsFor(userId).get(movieId);
        if (heldOut != null) {
            sum -= heldOut;
            count--;
        }
        return (sum + PRIOR_STRENGTH * globalMean) / (count + PRIOR_STRENGTH);
    }
}
