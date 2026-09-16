package com.demo.retrieval.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GrpoTopicAlignmentTest {

    @Test
    void servingPublishesToTheTopicTheOnlineJoinerSubscribesTo() {
        // OnlineJoinerStreamingJob.scala reads ONLINE_JOINER_INPUT_TOPIC, defaulting to
        // recsys_events. Serving must resolve the same variable to the same default: emitting to a
        // topic of its own means nothing consumes the events, and the training chain goes quiet
        // with no error anywhere to say so.
        String configured = System.getenv("ONLINE_JOINER_INPUT_TOPIC");
        assertEquals(configured == null ? "recsys_events" : configured,
            HybridRecommendationService.grpoOutputTopic());
    }
}
