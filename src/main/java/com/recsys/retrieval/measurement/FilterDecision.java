package com.demo.retrieval.measurement;

/** A bounded classification of an eligibility rejection; it never carries an item or user identifier. */
public record FilterDecision(String reason) {
}
