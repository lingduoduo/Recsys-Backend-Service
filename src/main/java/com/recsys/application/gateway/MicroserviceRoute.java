package com.recsys.application.gateway;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public record MicroserviceRoute(String name,
                         String prefix,
                         String envVar,
                         URI baseUri,
                         String healthPath,
                         String serviceName) {

    /** Convenience constructor for routes with no registry service name (serviceName = null). */
    public MicroserviceRoute(String name, String prefix, String envVar, URI baseUri, String healthPath) {
        this(name, prefix, envVar, baseUri, healthPath, null);
    }

    private static final List<MicroserviceRoute> DEFAULTS = buildDefaults();

    private static List<MicroserviceRoute> buildDefaults() {
        List<MicroserviceRoute> routes = new java.util.ArrayList<>();
        // Production recommendation routes
        routes.add(fromEnv("embed-recall",    "/api/recommend/embedding",   "EMBED_RECALL_SERVICE_URL",    "http://localhost:6010", "/health",       "recsys-catalog-serving"));
        routes.add(fromEnv("model-inference", "/api/recommend/model",       "MODEL_INFERENCE_SERVICE_URL", "http://localhost:8080", "/health/ready", "recsys-model-serving"));
        routes.add(fromEnv("online-blend",    "/api/recommend/online",      "ONLINE_BLEND_SERVICE_URL",    "http://localhost:7010", "/health",       "recsys-online-serving"));
        routes.add(fromEnv("sequential",      "/api/recommend/sequential",  "SEQUENTIAL_SERVICE_URL",      "http://localhost:8080", "/health/ready", "recsys-model-serving"));
        // Data / catalog routes
        routes.add(fromEnv("user-profile",    "/api/users",    "USER_PROFILE_SERVICE_URL",    "http://localhost:6010", "/health",       "recsys-catalog-serving"));
        routes.add(fromEnv("movie-metadata",  "/api/movies",   "MOVIE_METADATA_SERVICE_URL",  "http://localhost:6010", "/health",       "recsys-catalog-serving"));
        routes.add(fromEnv("feature",         "/api/features", "FEATURE_SERVICE_URL",          "http://localhost:7010", "/health",       "recsys-online-serving"));
        routes.add(fromEnv("knowledge",       "/api/knowledge","KNOWLEDGE_SERVICE_URL",        "http://localhost:8080", "/health/ready", "recsys-model-serving"));
        // The merged retrieval surface, which serves /api/v1/retrieval/** inside the 8080 JVM.
        // The gateway path therefore doubles the segment — /api/retrieval/api/v1/retrieval/... —
        // exactly as /api/model/api/v1/recommend and /api/online/online/features already do.
        // That identity (gateway path == prefix + backend path) is not cosmetic: rewrite() only
        // strips the prefix, and BackendRoutePolicy.userScopedGatewayPaths derives the
        // never-public guard from it. Folding /api/v1/retrieval into baseUri would read better
        // and silently break both that guard and GatewayHealthService's health URL.
        routes.add(fromEnv("retrieval",       "/api/retrieval","RETRIEVAL_SERVICE_URL",        "http://localhost:8080", "/health/ready", "recsys-model-serving"));
        // Backward-compatible routes kept for existing clients and smoke tests.
        routes.add(fromEnv("catalog", "/api/catalog", "CATALOG_SERVICE_URL", "http://localhost:6010", "/health",       "recsys-catalog-serving"));
        routes.add(fromEnv("model",   "/api/model",   "MODEL_SERVICE_URL",   "http://localhost:8080", "/health/ready", "recsys-model-serving"));
        routes.add(fromEnv("online",  "/api/online",  "ONLINE_SERVICE_URL",  "http://localhost:7010", "/health",       "recsys-online-serving"));
        // LLM routes are optional — only registered when the env var is explicitly set.
        // To enable: export LLM_SERVICE_URL=http://localhost:11434 (requires Ollama or compatible endpoint).
        fromEnvOptional("llm-explanation", "/api/explanations", "LLM_EXPLANATION_SERVICE_URL", "/api/tags").ifPresent(routes::add);
        fromEnvOptional("llm", "/api/llm", "LLM_SERVICE_URL", "/api/tags").ifPresent(routes::add);
        return List.copyOf(routes);
    }

    public MicroserviceRoute {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(envVar, "envVar");
        Objects.requireNonNull(baseUri, "baseUri");
        Objects.requireNonNull(healthPath, "healthPath");
        if (!prefix.startsWith("/")) {
            throw new IllegalArgumentException("route prefix must start with /");
        }
        if (baseUri.getScheme() == null || baseUri.getHost() == null) {
            throw new IllegalArgumentException("route base URI must include scheme and host: " + baseUri);
        }
    }

    public static List<MicroserviceRoute> defaults() {
        return DEFAULTS;
    }

    static MicroserviceRoute match(List<MicroserviceRoute> routes, String path) {
        String normalizedPath = normalizePath(path);
        MicroserviceRoute best = null;
        for (MicroserviceRoute route : routes) {
            if (matchesPrefix(normalizedPath, route.prefix())
                    && (best == null || route.prefix().length() > best.prefix().length())) {
                best = route;
            }
        }
        return best;
    }

    URI rewrite(String requestPath, String query) {
        String normalizedPath = normalizePath(requestPath);
        if (!matchesPrefix(normalizedPath, prefix)) {
            throw new IllegalArgumentException("path does not match route " + prefix + ": " + requestPath);
        }

        String suffix = normalizedPath.substring(prefix.length());
        if (suffix.isBlank()) {
            suffix = "/";
        }
        String targetPath = joinPaths(baseUri.getPath(), suffix);
        return URI.create(newUri(baseUri, targetPath, query));
    }

    URI healthUri() {
        return URI.create(newUri(baseUri, joinPaths(baseUri.getPath(), healthPath), null));
    }

    private static MicroserviceRoute fromEnv(String name,
                                             String prefix,
                                             String envVar,
                                             String defaultBaseUri,
                                             String healthPath,
                                             String serviceName) {
        String raw = System.getenv().getOrDefault(envVar, defaultBaseUri);
        return new MicroserviceRoute(name, prefix, envVar, URI.create(raw), healthPath, serviceName);
    }

    private static java.util.Optional<MicroserviceRoute> fromEnvOptional(String name,
                                                                          String prefix,
                                                                          String envVar,
                                                                          String healthPath) {
        String raw = System.getenv(envVar);
        if (raw == null || raw.isBlank()) return java.util.Optional.empty();
        return java.util.Optional.of(new MicroserviceRoute(name, prefix, envVar, URI.create(raw), healthPath));
    }

    static boolean matchesPrefix(String path, String prefix) {
        return path.equals(prefix) || path.startsWith(prefix + "/");
    }

    static String normalizePath(String path) {
        if (path == null || path.isBlank()) {
            return "/";
        }
        return path.startsWith("/") ? path : "/" + path;
    }

    private static String joinPaths(String left, String right) {
        String base = (left == null || left.isBlank()) ? "" : left;
        String suffix = (right == null || right.isBlank()) ? "/" : right;
        if (!base.startsWith("/") && !base.isBlank()) {
            base = "/" + base;
        }
        if (!suffix.startsWith("/")) {
            suffix = "/" + suffix;
        }
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base + suffix;
    }

    private static String newUri(URI base, String path, String query) {
        StringBuilder builder = new StringBuilder();
        builder.append(base.getScheme().toLowerCase(Locale.ROOT)).append("://").append(base.getAuthority());
        builder.append(path);
        if (query != null && !query.isBlank()) {
            builder.append('?').append(query);
        }
        return builder.toString();
    }
}
