package com.recsys.api.online;

import com.recsys.application.retrieval.multichannel.RecallDegradationMetrics;
import com.recsys.application.retrieval.multichannel.RecallResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 7010 must export recall-degradation outcomes, as 6010 does. Before this, its recall service built a private
 * {@link RecallDegradationMetrics} that nothing registered, so a degraded recall — including the cold-user
 * fallback when Redis is down — never reached {@code /metrics}.
 */
class OnlineRecallMetricsWiringTest {

    @Test
    void recallOutcomeMetricsAreRegisteredOnTheServingRegistry() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecallDegradationMetrics metrics = OnlinePredictionServer.createRecallMetrics(registry);

        metrics.recordOutcome(RecallResult.DegradationOutcome.PARTIAL);

        assertThat(registry.find("recsys.recall.degradation.outcomes")
                .tag("outcome", "partial").functionCounter())
                .as("partial-outcome counter on the 7010 registry").isNotNull();
        assertThat(registry.get("recsys.recall.degradation.outcomes")
                .tag("outcome", "partial").functionCounter().count()).isEqualTo(1.0);
    }
}
