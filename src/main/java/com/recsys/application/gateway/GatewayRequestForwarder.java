package com.recsys.application.gateway;

import com.recsys.application.auth.AdminTokenGuard;
import com.recsys.ratelimit.GatewayRateLimiter;
import com.recsys.ratelimit.TokenBucket;
import com.recsys.resilience.RouteCircuitBreaker;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.client.retry.Backoff;
import com.linecorp.armeria.client.retry.RetryRule;
import com.linecorp.armeria.client.retry.RetryingClient;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.server.ServiceRequestContext;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public final class GatewayRequestForwarder implements java.io.Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(GatewayRequestForwarder.class);

    private static final int SC_TOO_MANY_REQUESTS = 429;
    private static final Set<String> HOP_BY_HOP = Set.of(
            "connection", "content-length", "expect", "host", "keep-alive",
            "proxy-authenticate", "proxy-authorization", "te", "trailer",
            "transfer-encoding", "upgrade");

    // Credentials the gateway consumes at its auth boundary — never forwarded upstream.
    private static final Set<String> GATEWAY_CONSUMED_CREDENTIALS =
            Set.of("authorization", "x-api-key", GatewayOriginSecret.HEADER);

    private final UpstreamEndpointGroups staticUpstreams;     // non-null when registry disabled
    private final RegistryBackedUpstreams registryUpstreams;  // non-null when registry enabled
    private final Map<String, RouteCircuitBreaker> circuitBreakers;
    private final GatewayRateLimiter rateLimiter;
    private final Counter userScopeRejected;   // null when no registry was supplied
    private final GatewayCircuitMetrics circuitMetrics;   // null when no registry was supplied
    private final AtomicBoolean userScopeWarned = new AtomicBoolean();
    private final AdminTokenGuard operatorGuard;   // null means not configured, so nobody passes

    public GatewayRequestForwarder(List<MicroserviceRoute> routes,
                                   Duration timeout,
                                   Map<String, RouteCircuitBreaker> circuitBreakers,
                                   GatewayRateLimiter rateLimiter) {
        this(routes, timeout, circuitBreakers, rateLimiter, (MeterRegistry) null);
    }

    /** @param registry may be null, in which case denials are not counted. */
    public GatewayRequestForwarder(List<MicroserviceRoute> routes,
                                   Duration timeout,
                                   Map<String, RouteCircuitBreaker> circuitBreakers,
                                   GatewayRateLimiter rateLimiter,
                                   MeterRegistry registry) {
        this(routes, timeout, circuitBreakers, rateLimiter,
                UpstreamEndpointGroups.HealthCheckConfig.fromEnvironment(), registry);
    }

    /**
     * @param registry may be null, in which case denials are not counted.
     * @param operatorGuard gates OPERATOR-class routes; null means "not configured" and therefore
     *                      denies every such request.
     */
    public GatewayRequestForwarder(List<MicroserviceRoute> routes,
                                   Duration timeout,
                                   Map<String, RouteCircuitBreaker> circuitBreakers,
                                   GatewayRateLimiter rateLimiter,
                                   MeterRegistry registry,
                                   AdminTokenGuard operatorGuard) {
        this(routes, timeout, circuitBreakers, rateLimiter,
                UpstreamEndpointGroups.HealthCheckConfig.fromEnvironment(), registry, operatorGuard);
    }

    // Package-private: lets tests inject an explicit health-check config (e.g. a short probe interval).
    GatewayRequestForwarder(List<MicroserviceRoute> routes,
                            Duration timeout,
                            Map<String, RouteCircuitBreaker> circuitBreakers,
                            GatewayRateLimiter rateLimiter,
                            UpstreamEndpointGroups.HealthCheckConfig healthConfig) {
        this(routes, timeout, circuitBreakers, rateLimiter, healthConfig, null);
    }

    GatewayRequestForwarder(List<MicroserviceRoute> routes,
                            Duration timeout,
                            Map<String, RouteCircuitBreaker> circuitBreakers,
                            GatewayRateLimiter rateLimiter,
                            UpstreamEndpointGroups.HealthCheckConfig healthConfig,
                            MeterRegistry registry) {
        this(routes, timeout, circuitBreakers, rateLimiter, healthConfig, registry, null);
    }

    // Canonical constructor for the static-upstream path: every other overload delegates here.
    GatewayRequestForwarder(List<MicroserviceRoute> routes,
                            Duration timeout,
                            Map<String, RouteCircuitBreaker> circuitBreakers,
                            GatewayRateLimiter rateLimiter,
                            UpstreamEndpointGroups.HealthCheckConfig healthConfig,
                            MeterRegistry registry,
                            AdminTokenGuard operatorGuard) {
        this.circuitBreakers = Map.copyOf(circuitBreakers);
        this.rateLimiter = rateLimiter == null ? GatewayRateLimiter.disabled() : rateLimiter;
        this.staticUpstreams = UpstreamEndpointGroups.create(routes, timeout, retryDecorator(), healthConfig);
        this.registryUpstreams = null;
        this.userScopeRejected = counter(registry);
        this.circuitMetrics = GatewayCircuitMetrics.create(registry);
        this.operatorGuard = operatorGuard;
    }

    // Package-private: upstreams are registry-overlaid; built via registryBacked(...). Canonical
    // constructor for the registry-backed path.
    private GatewayRequestForwarder(Map<String, RouteCircuitBreaker> circuitBreakers,
                                    GatewayRateLimiter rateLimiter,
                                    RegistryBackedUpstreams registryUpstreams,
                                    MeterRegistry registry,
                                    AdminTokenGuard operatorGuard) {
        this.circuitBreakers = Map.copyOf(circuitBreakers);
        this.rateLimiter = rateLimiter == null ? GatewayRateLimiter.disabled() : rateLimiter;
        this.staticUpstreams = null;
        this.registryUpstreams = registryUpstreams;
        this.userScopeRejected = counter(registry);
        this.circuitMetrics = GatewayCircuitMetrics.create(registry);
        this.operatorGuard = operatorGuard;
    }

    /**
     * Builds a forwarder whose upstreams are overlaid with registry-resolved addresses.
     *
     * @param registry may be null, in which case denials are not counted.
     * @param operatorGuard gates OPERATOR-class routes; null means "not configured" and therefore
     *                      denies every such request.
     */
    public static GatewayRequestForwarder registryBacked(
            List<MicroserviceRoute> routes, Duration timeout,
            Map<String, RouteCircuitBreaker> circuitBreakers, GatewayRateLimiter rateLimiter,
            com.recsys.infrastructure.registry.ServiceRegistryProvider provider,
            MeterRegistry registry,
            AdminTokenGuard operatorGuard) {
        RegistryBackedUpstreams upstreams = new RegistryBackedUpstreams(
                routes, timeout, retryDecorator(),
                UpstreamEndpointGroups.HealthCheckConfig.fromEnvironment(), provider);
        return new GatewayRequestForwarder(circuitBreakers, rateLimiter, upstreams, registry, operatorGuard);
    }

    private static Counter counter(MeterRegistry registry) {
        return registry == null ? null
                : Counter.builder("gateway_user_scope_rejected_total")
                        .description("Requests rejected because the caller named a userId that is not their own")
                        .register(registry);
    }

    /**
     * Rebuilds the upstream endpoint groups if registry-resolved addresses have changed. Wired to the
     * {@code ServiceRegistryProvider}'s onRefresh callback; a no-op when the registry is disabled.
     */
    public void rebuildUpstreamsIfChanged() {
        if (registryUpstreams != null) {
            registryUpstreams.rebuildIfChanged();
        }
    }

    // One retry on IOException (not on timeout): Cloud Map instance registration/deregistration
    // causes a brief window where DNS resolves to a departing endpoint. A single retry after
    // 50 ms recovers from that transient failure without significantly increasing tail latency.
    private static Function<? super com.linecorp.armeria.client.HttpClient, RetryingClient> retryDecorator() {
        RetryRule retryRule = RetryRule.builder()
                .onException((ctx, cause) ->
                        cause instanceof java.io.IOException
                                && !(cause instanceof java.net.SocketTimeoutException))
                .thenBackoff(Backoff.fixed(50));
        return RetryingClient.builder(retryRule)
                .maxTotalAttempts(2)
                .newDecorator();
    }

    private WebClient clientFor(String routeName) {
        return registryUpstreams != null
                ? registryUpstreams.clientFor(routeName)
                : staticUpstreams.clientFor(routeName);
    }

    @Override
    public void close() {
        if (registryUpstreams != null) {
            registryUpstreams.close();
        } else {
            staticUpstreams.close();
        }
    }

    /**
     * Denies a request the gateway is not willing to proxy.
     *
     * <p>An allow-list: a path with no policy is denied, so a backend route added without a
     * classification is unreachable through the gateway rather than exposed by it. Routes that
     * resolve to no known backend — a genuine LLM upstream — are outside the table's remit and
     * pass through.
     *
     * <p>Two of the three denials — the unclassified path and the {@code NO_PROXY} one — return
     * the unrouted-path 404 verbatim. A path that exists but is withheld must not be
     * distinguishable from one that was never routed. The {@code OPERATOR} denial is deliberately
     * distinct (403, {@code operator token required}): that caller is being told to present a
     * credential, not that the path is absent, and the route it names is already published in the
     * runbooks, so there is no existence to conceal.
     *
     * @return the denial to return, or null when the request may proceed
     */
    HttpResponse enforceRoutePolicy(MicroserviceRoute route,
                                    String targetPath,
                                    AggregatedHttpRequest request,
                                    GatewayPrincipal principal) {
        String service = BackendRoutePolicy.effectiveServiceName(route, MicroserviceRoute.defaults());
        if (service == null) {
            return null;
        }
        BackendRoutePolicy.Policy policy = BackendRoutePolicy.lookup(
                service, BackendRoutePolicy.pathWithoutQuery(targetPath));
        if (policy == null || policy.access() == BackendRoutePolicy.Access.NO_PROXY) {
            return GatewayProxyService.gatewayError(HttpStatus.NOT_FOUND, "no route found");
        }
        if (policy.access() == BackendRoutePolicy.Access.OPERATOR) {
            // Tier-independent on purpose: an API key is what every real caller holds, so if it
            // were sufficient here the class would mean nothing. Unset token authorizes nobody.
            String presented = request.headers().get(AdminTokenGuard.HEADER);
            if (operatorGuard == null || !operatorGuard.isAuthorized(presented)) {
                return GatewayProxyService.gatewayError(
                        HttpStatus.FORBIDDEN, "operator token required");
            }
            return null;
        }
        return authorizeUserScope(route, targetPath, request, principal);
    }

    /**
     * Denies a user-tier caller that names a userId other than its own.
     *
     * <p>Service-tier callers — API keys and, in dev, anonymous — are exempt: the trust model is
     * that they are backends legitimately acting for many users. Routes absent from
     * {@link BackendRoutePolicy} are not user-scoped and are never checked.
     *
     * @return the 403 to return, or null when the request may proceed
     */
    HttpResponse authorizeUserScope(MicroserviceRoute route,
                                    String targetPath,
                                    AggregatedHttpRequest request,
                                    GatewayPrincipal principal) {
        if (principal == null || principal.tier() != GatewayPrincipal.Tier.USER) {
            return null;
        }
        // effectiveServiceName, not route.serviceName(): a route that declares no registry name
        // still reaches a backend, and declining to name itself must not be a way out of the check.
        BackendRoutePolicy.Policy policy = BackendRoutePolicy.lookup(
                BackendRoutePolicy.effectiveServiceName(route, MicroserviceRoute.defaults()),
                BackendRoutePolicy.pathWithoutQuery(targetPath));
        if (policy == null || policy.access() != BackendRoutePolicy.Access.USER_SCOPED) {
            return null;
        }
        UserIdSource source = policy.userIdSource();
        String requested = source.extract(targetPath, request);
        // Blank on either side is a denial, not an exemption: a subject we cannot determine is a
        // request we cannot authorize, so we authorize before the backend gets to validate.
        if (principal.appUserId().isBlank()
                || requested.isBlank()
                || !requested.equals(principal.appUserId())) {
            if (userScopeRejected != null) {
                userScopeRejected.increment();
            }
            // Logged once, like GatewayOriginSecret: under a broken claim mapping this fires on
            // every request. Neither id is logged — the counter is the signal.
            if (userScopeWarned.compareAndSet(false, true)) {
                LOG.warn("Rejected a user-scoped request whose userId is not the caller's (first "
                                + "occurrence, route={}, principal={}). If this began at a "
                                + "deployment, GATEWAY_COGNITO_USER_ID_CLAIM and the user pool "
                                + "disagree. Further rejections are counted in "
                                + "gateway_user_scope_rejected_total and not logged.",
                        route.name(), principal.rateLimitKey());
            }
            return GatewayProxyService.gatewayError(HttpStatus.FORBIDDEN,
                    "forbidden: request is not scoped to the authenticated user");
        }
        return null;
    }

    HttpResponse forward(ServiceRequestContext ctx,
                         AggregatedHttpRequest request,
                         MicroserviceRoute route,
                         String targetPath,
                         GatewayPrincipal principal) {
        TokenBucket.Decision rateDecision = rateLimiter.tryAcquire(route.name(), principal.rateLimitKey());
        if (!rateDecision.allowed()) {
            int retryAfter = Math.max(1, (int) Math.ceil(rateDecision.retryAfter().toMillis() / 1000.0));
            return HttpResponse.of(
                    ResponseHeaders.builder(HttpStatus.valueOf(SC_TOO_MANY_REQUESTS))
                            .contentType(MediaType.JSON_UTF_8)
                            .set(HttpHeaderNames.RETRY_AFTER, String.valueOf(retryAfter))
                            .set(HttpHeaderNames.of("x-ratelimit-limit"), String.valueOf(rateDecision.limit()))
                            .set(HttpHeaderNames.of("x-ratelimit-remaining"), String.valueOf(rateDecision.remaining()))
                            .build(),
                    HttpData.ofUtf8("{\"error\":\"" + route.name() + " gateway rate limited\"}"));
        }

        // After rate limiting, so a probing caller still spends their own tokens on each denial.
        // Before the circuit-breaker permit, because success/failure is recorded only on the
        // upstream-response path below — returning 403 holding a permit would leak it. Worse than a
        // leak on a HALF_OPEN route: the permit claims the single probe slot, so an unsettled one
        // wedges the route into permanent 503s. Pinned by
        // UserScopeAuthorizationTest#aDenialNeverConsumesTheCircuitBreakerProbeSlot.
        HttpResponse denied = enforceRoutePolicy(route, targetPath, request, principal);
        if (denied != null) {
            return denied;
        }

        RouteCircuitBreaker cb = circuitBreakers.get(route.name());
        RouteCircuitBreaker.Permit permit = cb == null ? null : cb.tryAcquirePermit();
        if (cb != null && permit == null) {
            return GatewayProxyService.gatewayError(HttpStatus.SERVICE_UNAVAILABLE,
                    route.name() + " circuit open — upstream unavailable, retry later");
        }

        WebClient client = clientFor(route.name());
        RequestHeaders upstreamHeaders = buildUpstreamHeaders(request.headers(), targetPath, ctx, principal);
        HttpRequest upstreamReq = HttpRequest.of(upstreamHeaders, request.content());
        HttpResponse upstream = client.execute(upstreamReq);
        return GatewayUpstreamResponse.relay(upstream, cb, permit, route.name(), circuitMetrics);
    }

    static RequestHeaders buildUpstreamHeaders(RequestHeaders incoming, String targetPath,
                                               ServiceRequestContext ctx, GatewayPrincipal principal) {
        RequestHeadersBuilder b = RequestHeaders.builder(incoming.method(), targetPath);
        incoming.forEach((name, value) -> {
            String n = name.toString();
            // Strip any client-supplied identity header — the gateway is the sole authority.
            if (!isHopByHop(n) && !isGatewayCredential(n)
                    && !n.regionMatches(true, 0, "x-authenticated-", 0, 16)) {
                b.add(name, value);
            }
        });
        principal.identityHeaders().forEach((hn, hv) -> b.set(HttpHeaderNames.of(hn), hv));
        b.set(HttpHeaderNames.of("x-gateway-service"), "recsys-api-gateway");
        String host = incoming.get(HttpHeaderNames.HOST);
        if (host != null && !host.isBlank()) {
            b.set(HttpHeaderNames.of("x-forwarded-host"), host);
        }
        b.set(HttpHeaderNames.of("x-forwarded-proto"), "http");
        if (ctx.remoteAddress() != null) {
            String clientIp = ctx.remoteAddress().getAddress().getHostAddress();
            String existing = incoming.get(HttpHeaderNames.of("x-forwarded-for"));
            String newValue = (existing != null && !existing.isBlank())
                    ? existing + ", " + clientIp : clientIp;
            b.set(HttpHeaderNames.of("x-forwarded-for"), newValue);
        }
        return b.build();
    }

    private static boolean isHopByHop(String name) {
        return name != null && HOP_BY_HOP.contains(name.toLowerCase(Locale.ROOT));
    }

    private static boolean isGatewayCredential(String name) {
        return name != null && GATEWAY_CONSUMED_CREDENTIALS.contains(name.toLowerCase(Locale.ROOT));
    }
}
