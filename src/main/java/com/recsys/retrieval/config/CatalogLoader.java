package com.demo.retrieval.config;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Loads an optional external catalog JSON ({@code recsys.catalog-path}) and merges it on top of
 * the inline {@code recsys.catalog} at startup. Lets the item catalog outgrow application.yml and
 * stay in sync with the Python pipeline's catalog without a code change. No-op when the path is
 * unset; a missing file is logged and skipped (the inline catalog still serves).
 */
@Component
public class CatalogLoader {
    private static final Logger log = LoggerFactory.getLogger(CatalogLoader.class);

    private final RecommendationProperties properties;
    private final ObjectMapper objectMapper;

    public CatalogLoader(RecommendationProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void loadExternalCatalog() {
        String catalogPath = properties.getCatalogPath();
        if (catalogPath == null || catalogPath.isBlank()) {
            return;
        }
        Path path = Path.of(catalogPath);
        if (!Files.isRegularFile(path)) {
            log.warn("recsys.catalog-path={} does not exist; keeping inline catalog ({} items)",
                catalogPath, properties.getCatalog().size());
            return;
        }
        try {
            Map<String, MovieProfile> external = objectMapper.readValue(
                path.toFile(), new TypeReference<Map<String, MovieProfile>>() {});
            properties.getCatalog().putAll(external);
            log.info("Merged {} catalog entries from {} (catalog now {} items)",
                external.size(), catalogPath, properties.getCatalog().size());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load catalog from " + catalogPath, e);
        }
    }
}
