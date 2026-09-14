package com.recsys.infrastructure.vectordb.spann;

/** Point-in-time counters of a {@link SpannVectorIndex}; the seam Prometheus would attach to. */
public record SpannStats(int centroidsLive, int centroidsTotal, int entriesLive, long fileBytes, long deadBytes,
                         long splits, long merges, long reassigned, long compactions,
                         long distanceComputations, long fallbackWidenings) {}
