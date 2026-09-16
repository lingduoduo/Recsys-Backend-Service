package com.recsys.retrieval.model;

public record ModelPrediction(
    String user,
    String item,
    long userId,
    long itemId,
    double score,
    String model
) {
}
