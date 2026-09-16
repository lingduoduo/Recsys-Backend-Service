package com.recsys.retrieval.kafka.event;

public sealed interface MovieLensEvent
        permits UserEvent, MovieEvent, RatingEvent, MovieInteractionEvent {}
