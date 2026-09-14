package com.recsys.api.serving;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import com.recsys.domain.item.Movie;
import com.recsys.infrastructure.dataloading.DataManager;
import com.recsys.infrastructure.vectordb.EmbeddingStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Fixture where inner product and Sum of MaxSim disagree. Seed 1 = [1,0] with co-rated
 * neighbour 10 = [0,1], so the seed bag is {[1,0],[0,1]}. Candidate 2 = [0.9,0.2] is the
 * closest single vector (0.9) but has no neighbours: MaxSim 0.9 + 0.2 = 1.1. Candidate 3 =
 * [0.6,0.2] is further away (0.6) but shares neighbour 10: MaxSim 0.6 + 1.0 = 1.6.
 */
class SimilarMaxSimScoringTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    static final DataManager mockData = mock(DataManager.class);
    static final EmbeddingStore mockEmb = mock(EmbeddingStore.class);

    private static Movie movie(int id) {
        return new Movie(id, "m" + id, 2000, List.of());
    }

    static {
        when(mockData.getSimilarMovies(anyInt())).thenReturn(List.of());
        when(mockData.getSimilarMovies(1)).thenReturn(List.of(movie(10)));
        when(mockData.getSimilarMovies(3)).thenReturn(List.of(movie(10)));
        when(mockData.getTopRatedMovies(anyInt())).thenReturn(List.of(movie(2), movie(3), movie(10)));
        when(mockData.getMoviesByGenre(any(), anyInt())).thenReturn(List.of());
        when(mockData.getMovieById(anyInt())).thenReturn(null);
        when(mockData.getMovieById(1)).thenReturn(movie(1));

        Map<Integer, float[]> vectors = Map.of(
                1, new float[]{1f, 0f},
                2, new float[]{0.9f, 0.2f},
                3, new float[]{0.6f, 0.2f},
                10, new float[]{0f, 1f});
        when(mockEmb.getEmbedding(anyInt())).thenReturn(null);
        when(mockEmb.getEmbedding(1)).thenReturn(vectors.get(1));
        when(mockEmb.getEmbeddings(any())).thenAnswer(inv -> {
            Map<Integer, float[]> out = new java.util.HashMap<>();
            for (Integer id : inv.<java.util.Collection<Integer>>getArgument(0)) {
                if (vectors.containsKey(id)) out.put(id, vectors.get(id));
            }
            return out;
        });
    }

    @RegisterExtension
    static final ServerExtension server = new ServerExtension() {
        @Override
        protected void configure(ServerBuilder sb) {
            sb.service("/similar", new RecommendationService.Similar(mockEmb, mockData));
            sb.service("/similar-maxsim", new RecommendationService.Similar(
                    mockEmb, mockData, RecommendationService.Similar.Scoring.SUM_OF_MAXSIM));
        }
    };

    private static List<Integer> ids(AggregatedHttpResponse res) throws Exception {
        List<Integer> out = new ArrayList<>();
        for (JsonNode n : JSON.readTree(res.contentUtf8()).get("similar")) out.add(n.get("movieId").asInt());
        return out;
    }

    @Test
    void defaultScoring_ranksByInnerProduct() throws Exception {
        AggregatedHttpResponse res = WebClient.of(server.httpUri()).get("/similar?movieId=1&k=3").aggregate().join();
        assertThat(res.status()).isEqualTo(HttpStatus.OK);
        assertThat(ids(res)).containsExactly(2, 3, 10);
    }

    @Test
    void maxSimScoring_ranksByNeighbourOverlap() throws Exception {
        AggregatedHttpResponse res = WebClient.of(server.httpUri()).get("/similar-maxsim?movieId=1&k=3").aggregate().join();
        assertThat(res.status()).isEqualTo(HttpStatus.OK);
        assertThat(ids(res)).containsExactly(3, 2, 10);
        JsonNode top = JSON.readTree(res.contentUtf8()).get("similar").get(0);
        assertThat(top.get("score").asDouble()).isCloseTo(1.6, within(1e-6));
    }

    @Test
    void maxSimScoring_keepsTheSameCacheContract() {
        AggregatedHttpResponse res = WebClient.of(server.httpUri()).get("/similar-maxsim?movieId=1&k=3").aggregate().join();
        assertThat(res.headers().get(HttpHeaderNames.CACHE_CONTROL))
                .isEqualTo("public, s-maxage=300, stale-while-revalidate=3600, stale-if-error=3600");
        assertThat(res.headers().get(HttpHeaderNames.ETAG)).isNotBlank();
    }

    @Test
    void maxSimScoring_missingSeedEmbeddingIsStill404NoStore() {
        AggregatedHttpResponse res = WebClient.of(server.httpUri()).get("/similar-maxsim?movieId=999").aggregate().join();
        assertThat(res.status()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(res.headers().get(HttpHeaderNames.CACHE_CONTROL)).isEqualTo("no-store");
    }

    @Test
    void scoringFromEnv_defaultsToInnerProduct() {
        assertThat(RecommendationService.Similar.Scoring.fromEnv(name -> null))
                .isEqualTo(RecommendationService.Similar.Scoring.INNER_PRODUCT);
        assertThat(RecommendationService.Similar.Scoring.fromEnv(name -> "  "))
                .isEqualTo(RecommendationService.Similar.Scoring.INNER_PRODUCT);
    }

    @Test
    void scoringFromEnv_acceptsSumOfMaxSimCaseInsensitively() {
        assertThat(RecommendationService.Similar.Scoring.fromEnv(name -> "Sum_Of_MaxSim"))
                .isEqualTo(RecommendationService.Similar.Scoring.SUM_OF_MAXSIM);
    }

    @Test
    void scoringFromEnv_rejectsUnknownValueInsteadOfSilentlyDefaulting() {
        // A typo must not quietly serve the default scorer under a flag that says otherwise.
        assertThatThrownBy(() -> RecommendationService.Similar.Scoring.fromEnv(name -> "maxsim"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_SIMILAR_SCORING");
    }
}
