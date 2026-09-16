package com.demo.retrieval.config;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class CatalogLoaderTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private RecommendationProperties propsWithInlineItem() {
        RecommendationProperties props = new RecommendationProperties();
        MovieProfile inline = new MovieProfile();
        inline.setTitle("Inline Movie");
        props.getCatalog().put("item1", inline);
        return props;
    }

    @Test
    void mergesExternalCatalogOnTopOfInline(@TempDir Path dir) throws Exception {
        Path catalog = dir.resolve("catalog.json");
        Files.writeString(catalog,
            "{\"item2\":{\"title\":\"External Movie\",\"genres\":[\"drama\"],\"newRelease\":true}}");

        RecommendationProperties props = propsWithInlineItem();
        props.setCatalogPath(catalog.toString());

        new CatalogLoader(props, mapper).loadExternalCatalog();

        assertThat(props.getCatalog()).containsKeys("item1", "item2");
        MovieProfile loaded = props.getCatalog().get("item2");
        assertThat(loaded.getTitle()).isEqualTo("External Movie");
        assertThat(loaded.getGenres()).containsExactly("drama");
        assertThat(loaded.isNewRelease()).isTrue();
    }

    @Test
    void externalEntryOverridesInlineWithSameId(@TempDir Path dir) throws Exception {
        Path catalog = dir.resolve("catalog.json");
        Files.writeString(catalog, "{\"item1\":{\"title\":\"Overridden\"}}");

        RecommendationProperties props = propsWithInlineItem();
        props.setCatalogPath(catalog.toString());

        new CatalogLoader(props, mapper).loadExternalCatalog();

        assertThat(props.getCatalog().get("item1").getTitle()).isEqualTo("Overridden");
    }

    @Test
    void noOpWhenCatalogPathBlank() {
        RecommendationProperties props = propsWithInlineItem();
        props.setCatalogPath("");

        new CatalogLoader(props, mapper).loadExternalCatalog();

        assertThat(props.getCatalog()).containsOnlyKeys("item1");
    }

    @Test
    void missingFileKeepsInlineCatalog(@TempDir Path dir) {
        RecommendationProperties props = propsWithInlineItem();
        props.setCatalogPath(dir.resolve("does-not-exist.json").toString());

        new CatalogLoader(props, mapper).loadExternalCatalog();

        assertThat(props.getCatalog()).containsOnlyKeys("item1");
    }
}
