package com.demo.retrieval.kafka.core;

import com.demo.retrieval.kafka.event.MovieLensEvent;
import com.demo.retrieval.kafka.event.RatingEvent;

public class MovieLensDeserializer {

    // Parses a CSV line from ratings.csv: userId,movieId,rating,timestamp
    public static MovieLensEvent deserialize(byte[] bytes) {
        String line = new String(bytes).strip();
        String[] parts = line.split(",", -1);
        if (parts.length != 4) {
            throw new IllegalArgumentException("Unexpected CSV format: " + line);
        }
        return new RatingEvent(
            parts[0].strip(),
            parts[1].strip(),
            Double.parseDouble(parts[2].strip()),
            Long.parseLong(parts[3].strip())
        );
    }
}
