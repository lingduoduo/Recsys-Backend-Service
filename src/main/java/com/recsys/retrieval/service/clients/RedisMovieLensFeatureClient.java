package com.demo.retrieval.service.clients;

import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.UserDemographics;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
public class RedisMovieLensFeatureClient implements MovieLensFeatureClient {
    private final StringRedisTemplate redis;

    public RedisMovieLensFeatureClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public Optional<MovieLensUserFeatures> getUserFeatures(String userId) {
        Map<Object, Object> raw = redis.opsForHash().entries("user:" + userId + ":features");
        if (raw == null || raw.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new MovieLensUserFeatures(
            userId,
            readList(raw.get("favoriteGenres")),
            readDouble(raw.get("avgRating")),
            (int) readLong(raw.get("ratingCount")),
            readList(raw.get("recentlyRatedMovieIds")),
            readList(raw.get("actionSequenceMovieIds")),
            readList(raw.get("retrievalSequenceMovieIds")),
            readList(raw.get("scoringSequenceMovieIds")),
            readList(raw.get("servedMovieIds")),
            readLongList(raw.get("pastRequestTimestamps")),
            readIntegerList(raw.get("inferredGenres")),
            readLongList(raw.get("impressionBloomFilter")),
            readList(raw.get("impressedMovieIds")),
            readList(raw.get("cachedMovieIds")),
            readBoolean(raw.get("hasCachedMovies")),
            new UserDemographics(
                (int) readLong(raw.get("age")),
                readString(raw.get("gender")),
                readString(raw.get("occupation")),
                readString(raw.get("zipCode"))
            )
        ));
    }

    private List<String> readList(Object raw) {
        if (raw == null) {
            return List.of();
        }
        return Arrays.stream(String.valueOf(raw).split(","))
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .toList();
    }

    private List<Integer> readIntegerList(Object raw) {
        if (raw == null) {
            return List.of();
        }
        return Arrays.stream(String.valueOf(raw).split(","))
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .map(this::parseNullableInteger)
            .filter(value -> value != null)
            .toList();
    }

    private List<Long> readLongList(Object raw) {
        if (raw == null) {
            return List.of();
        }
        return Arrays.stream(String.valueOf(raw).split(","))
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .map(this::parseNullableLong)
            .filter(value -> value != null)
            .toList();
    }

    private double readDouble(Object raw) {
        if (raw == null) {
            return 0.0;
        }
        try {
            return Double.parseDouble(String.valueOf(raw));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private boolean readBoolean(Object raw) {
        return raw != null && Boolean.parseBoolean(String.valueOf(raw));
    }

    private Double readNullableDouble(Object raw) {
        if (raw == null) {
            return null;
        }
        try {
            return Double.parseDouble(String.valueOf(raw));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private long readLong(Object raw) {
        Long parsed = parseNullableLong(raw);
        return parsed == null ? 0L : parsed;
    }

    private Long parseNullableLong(Object raw) {
        if (raw == null) {
            return null;
        }
        try {
            return Long.parseLong(String.valueOf(raw));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Integer parseNullableInteger(Object raw) {
        if (raw == null) {
            return null;
        }
        try {
            return Integer.parseInt(String.valueOf(raw));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String readString(Object raw) {
        return raw == null ? "" : String.valueOf(raw).trim();
    }

    private String readNullableString(Object raw) {
        String value = readString(raw);
        return value.isBlank() ? null : value;
    }
}
