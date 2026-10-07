package com.recsys.infrastructure.vectordb;

import com.recsys.infrastructure.dataloading.DataManager;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * The seed movie embeddings are 6-dimensional, so the in-memory ANN index is built for 6 dims.
 * updateEmbedding must reject a mismatched-dimension vector with a clear IllegalArgumentException
 * instead of letting it reach the LSH index and blow up with ArrayIndexOutOfBoundsException.
 */
class CandidateGeneratorDimensionTest {

    private final CandidateGenerator generator = new CandidateGenerator(mock(DataManager.class));

    @Test
    void updateEmbedding_rejectsDimensionMismatch_withClearMessage() {
        assertThatThrownBy(() -> generator.updateEmbedding(5, new float[]{0.1f, 0.3f, 0.6f}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimension mismatch")
                .hasMessageContaining("expected 6")
                .hasMessageContaining("got 3");
    }

    @Test
    void updateEmbedding_acceptsMatchingDimension() {
        assertThatCode(() -> generator.updateEmbedding(5, new float[]{0.1f, 0.3f, 0.6f, 0.0f, 0.0f, 0.0f}))
                .doesNotThrowAnyException();
    }

    @Test
    void seedEmbeddingsExposeTheClasspathVectorsReadOnly() {
        assertThat(generator.seedEmbeddings()).isNotEmpty();
        assertThat(generator.seedEmbeddings().values().iterator().next()).hasSize(generator.embeddingDimension());
        assertThatThrownBy(() -> generator.seedEmbeddings().put(-1, new float[0]))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
