package com.recsys.infrastructure.vectordb;

import com.recsys.infrastructure.dataloading.DataManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class CandidateGeneratorSpannWiringTest {

    @TempDir Path dir;

    @AfterEach
    void clearProperty() {
        System.clearProperty("recsys.vector.backend");
    }

    @Test
    void backendPropertySpann_buildsASpannIndexAndCloseRemovesItsFile() throws Exception {
        System.setProperty("recsys.vector.backend", "spann");
        CandidateGenerator gen = new CandidateGenerator(mock(DataManager.class), null, dir);
        assertThat(gen.embeddingBackendName()).isEqualTo("spann");
        assertThat(Files.list(dir).count()).isEqualTo(1);
        gen.close();
        assertThat(Files.list(dir).count()).isZero();
    }

    @Test
    void defaultBackend_isStillLsh_andCloseIsANoOp() {
        CandidateGenerator gen = new CandidateGenerator(mock(DataManager.class));
        assertThat(gen.embeddingBackendName()).isEqualTo("lsh");
        gen.close();
    }
}
