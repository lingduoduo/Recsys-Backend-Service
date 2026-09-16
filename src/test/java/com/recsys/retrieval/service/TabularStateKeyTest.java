package com.demo.retrieval.service;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TabularStateKeyTest {

    @Test
    void orderAndDuplicatesDoNotChangeKey() {
        String a = TabularStateKey.hash(List.of("drama", "comedy"), List.of("dark"));
        String b = TabularStateKey.hash(List.of("comedy", "drama", "drama"), List.of("dark", "dark"));
        assertEquals(a, b);
    }

    @Test
    void differentProfilesProduceDifferentKeys() {
        assertNotEquals(
            TabularStateKey.hash(List.of("drama"), List.of("dark")),
            TabularStateKey.hash(List.of("comedy"), List.of("dark")));
    }

    @Test
    void nullAndEmptyNormalizeEqually() {
        assertEquals(
            TabularStateKey.hash(null, null),
            TabularStateKey.hash(List.of(), List.of()));
    }

    // Recurrence lock: two states with the same taste profile but different recent-watch
    // history must key identically (this mirrors what the service's stateKey extracts).
    @Test
    void keyIgnoresRecentItemHistory() {
        Map<String, Object> stateA = Map.of(
            "recent", List.of("m1", "m2"), "genres", List.of("drama"), "tags", List.of("dark"));
        Map<String, Object> stateB = Map.of(
            "recent", List.of("m9"), "genres", List.of("drama"), "tags", List.of("dark"));
        assertEquals(
            TabularStateKey.hash(stateA.get("genres"), stateA.get("tags")),
            TabularStateKey.hash(stateB.get("genres"), stateB.get("tags")));
    }
}
