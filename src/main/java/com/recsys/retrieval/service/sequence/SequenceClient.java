package com.demo.retrieval.service.sequence;

import java.time.Duration;
import java.util.Set;

public interface SequenceClient {

    /**
     * Reads up to {@code maxRows} of a user's sequence, newest first, looking back no further
     * than {@code lookback}. Only {@code columns} are populated in the result.
     */
    SequenceSlice read(String userId, String kind, Set<String> columns, int maxRows, Duration lookback);
}
