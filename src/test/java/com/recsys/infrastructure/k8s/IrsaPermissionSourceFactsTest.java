package com.recsys.infrastructure.k8s;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The IRSA comments are provisioning instructions; this pins the source facts they assert.
 *
 * <p>{@code k8s/eks-shared/gateway-irsa-sa.yaml}, {@code k8s/eks-shared/patches/irsa-model-serving.yaml},
 * {@code k8s/base/online-serving.yaml} and {@code k8s/base/outbox-relay-deployment.yaml} tell whoever
 * provisions an IAM role which AWS permissions each workload needs. Nobody applies them by machine, so
 * they are only as good as the claims in them — and a wrong claim fails late and expensively: an
 * over-grant hands a public-facing pod permissions it never uses, an under-grant surfaces as
 * {@code AccessDenied} the first time a feature flag is flipped in production.
 *
 * <p>They were wrong twice. The gateway comment asked for two {@code servicediscovery} and three
 * {@code route53} permissions, including {@code ChangeResourceRecordSets}, for a workload that makes no
 * AWS API call at all. And online serving — a fourth SQS-calling workload — went undocumented through
 * two reviews of the change that corrected the other three, because nothing checked the claims
 * mechanically and each review re-derived the workload list by hand.
 *
 * <p><strong>What this pins, and what it deliberately does not.</strong> Asserting that a comment
 * contains particular words would pin prose: it would break every time the wording improved, while
 * still passing for a comment that is fluent and false. So this asserts the facts underneath the
 * wording instead:
 *
 * <ol>
 *   <li>{@link #sqsIsTheOnlyAwsServiceClientInMainSource()} — the only AWS SDK v2 <em>service</em>
 *       client anywhere in {@code src/main/java} is {@code sqs}. This is the claim the gateway comment
 *       rests on; adding a {@code servicediscovery}, {@code route53} or {@code s3} client falsifies it
 *       and fails here, at the commit that introduces the client rather than at the review that missed
 *       it.
 *   <li>{@link #everySqsSendCallSiteIsAttributedToAWorkload()} — the set of files that send to SQS, and
 *       which action each one calls, equals {@link #SEND_CALL_SITES}. This pins the <em>action</em>: a
 *       publisher that switches between {@code SendMessage} and {@code SendMessageBatch} invalidates
 *       the action named in its workload's comment.
 *   <li>{@link #everySqsClientConstructionSiteIsAttributedToAWorkload()} — the set of files that
 *       <em>construct</em> an SQS client or an SQS-backed publisher equals {@link #CONSTRUCTION_SITES}.
 *       This pins the <em>workload list</em>, and it is the check that would have caught the
 *       online-serving omission on its own. Check 2 cannot: a workload that reuses an existing
 *       publisher class adds no {@code sendMessage(} file and changes no action set, which is exactly
 *       how online serving was wired — {@code OnlinePredictionServer} names no SQS type at all, only
 *       {@code AsyncEventPublisherFactory.fromEnvironment("ONLINE_EVENTS")}. Every entry carries a note
 *       naming the workload it belongs to, so a new one fails the build until somebody says which role
 *       needs which permission.
 * </ol>
 *
 * <p>The action distinction in {@link #SEND_CALL_SITES} is load-bearing, not decoration:
 * {@code sqs:SendMessage} and {@code sqs:SendMessageBatch} are distinct IAM actions, and a role granting
 * only the first fails against a publisher that batches. That is exactly the error the model-serving
 * comment shipped with in its first draft.
 *
 * <p><strong>How a fact is found.</strong> All three scans match text anywhere in a source file — the
 * SDK package prefix, the bare {@code sendMessage(} / {@code sendMessageBatch(} call shapes, and the
 * construction shapes in {@link #CONSTRUCTION_MARKERS} — rather than a parsed import or resolved call.
 * That deliberately over-matches, exactly as {@link OperatorTokenManifestTest}'s marker does: a
 * fully-qualified client built inline, or a send behind a helper, is a real permission need that an
 * import-only scan would miss, and the trade is asymmetric. A false positive (a javadoc naming a
 * client, a string literal) is a loud failure a human clears in one line by adding the entry with a
 * note saying so. A false negative is a workload sending to a queue its role cannot write, discovered
 * in production. Files are keyed by their path relative to {@code src/main/java}, not by bare filename,
 * so two same-named classes in different packages cannot collide into one entry.
 *
 * <p><strong>What these checks still cannot catch.</strong> They are one-directional in the same sense
 * as {@link OperatorTokenManifestTest}: they prove the comments are not missing a caller, never that a
 * comment's prose is accurate. Specifically, all three stay green through:
 *
 * <ul>
 *   <li><strong>Any SQS action that is not a send.</strong> {@code ReceiveMessage},
 *       {@code DeleteMessage}, {@code GetQueueUrl}, {@code ChangeMessageVisibility} and the rest are
 *       distinct IAM actions and no check looks for them. A consumer added to any workload above needs
 *       its permissions written into the manifest comment by hand; nothing here will ask.
 *   <li><strong>A wrong workload note.</strong> Nothing verifies that {@code SqsAsyncEventPublisher}
 *       really is reached from the two workloads its note names, or that
 *       {@code AsyncEventPublisherFactory} really has only the one caller. A note naming the wrong
 *       Deployment is a passing test and a misleading comment. The note exists to force the question at
 *       review time, which is more than existed before, but it is not proof.
 *   <li><strong>A workload reaching SQS through a factory this file does not know.</strong>
 *       {@link #CONSTRUCTION_MARKERS} is a hand-maintained list of shapes. It covers both factories
 *       that exist today, so check 3 closes the gap that let online serving through — but a third
 *       factory added later, wrapping the client behind a name not in that list, reintroduces the same
 *       blind spot one level up. Check 1 still catches its {@code SqsClient} construction wherever that
 *       ultimately lives; what escapes is the attribution — which workload calls it, and therefore
 *       which role needs the permission. Add the new factory's shape to the list in the commit that
 *       introduces it.
 *   <li><strong>AWS SDK v1.</strong> {@link #AWS_SERVICE_PACKAGE} matches
 *       {@code software.amazon.awssdk.services.*} only. A {@code com.amazonaws.services.s3} client —
 *       v1, a different artifact, the same IAM consequence — falsifies the gateway's "SQS is the only
 *       client" premise and every check here stays green.
 *   <li><strong>Anything outside {@code src/main/java}.</strong> Test sources are not scanned, which is
 *       intended: a fake or a test-only client needs no IAM. But neither is anything else — a
 *       {@code scripts/} helper, a sibling repo, an IaC template. Note that {@code online/flink/} and
 *       {@code training/embedding/} <em>are</em> scanned: they are excluded from the Maven compile, not
 *       from {@code src/main/java}, and {@code Files.walk} reads them like any other directory.
 * </ul>
 *
 * <p>All three checks are pure file parsing — no Redis, no Docker, no timing.
 */
class IrsaPermissionSourceFactsTest {

    private static final Path SOURCE_ROOT = Path.of("src", "main", "java");

    /**
     * Matches a reference to any AWS SDK v2 service client package, in an import or a fully-qualified
     * use. The captured group is the service name — {@code sqs}, {@code s3}, {@code route53}, …
     */
    private static final Pattern AWS_SERVICE_PACKAGE =
            Pattern.compile("software\\.amazon\\.awssdk\\.services\\.([a-zA-Z0-9]+)");

    /**
     * The one service the manifests account for. Everything the comments say about the gateway — and
     * the whole "no IAM needed" conclusion — depends on nothing else appearing here.
     */
    private static final Set<String> DOCUMENTED_SERVICES = Set.of("sqs");

    private static final String SEND_MESSAGE = "sendMessage";
    private static final String SEND_MESSAGE_BATCH = "sendMessageBatch";

    /**
     * Every file that sends to SQS, the action(s) it calls, and the workload whose IAM role therefore
     * needs that permission. Derived from the source, and the reason each manifest comment names the
     * action it names. Keep the note honest: it is what a reviewer reads when this test fails.
     */
    private static final Map<String, CallSite> SEND_CALL_SITES = Map.of(
            "com/recsys/application/outbox/SqsOutboxDeliveryAdapter.java", new CallSite(
                    Set.of(SEND_MESSAGE),
                    "outbox relay (k8s/base/outbox-relay-deployment.yaml) — constructed by "
                            + "OutboxRelayCommand under SAGA_EVENTS_SQS_ENABLED + a non-blank "
                            + "SAGA_EVENTS_SQS_QUEUE_URL, both off in k8s/base/configmap.yaml; the "
                            + "Deployment has no serviceAccountName, so there is no role to grant on yet"),
            "com/recsys/infrastructure/messaging/SqsAsyncEventPublisher.java", new CallSite(
                    Set.of(SEND_MESSAGE_BATCH),
                    "model serving (ModelEventConfig.abExposurePublisher, RECSYS_EVENTS_SQS_*, "
                            + "k8s/eks-shared/patches/irsa-model-serving.yaml) and online serving "
                            + "(AsyncEventPublisherFactory.fromEnvironment(\"ONLINE_EVENTS\"), "
                            + "ONLINE_EVENTS_SQS_*, k8s/base/online-serving.yaml) — batches "
                            + "unconditionally, so both need SendMessageBatch and not SendMessage"),
            "com/recsys/application/saga/SqsSagaEventPublisher.java", new CallSite(
                    Set.of(SEND_MESSAGE),
                    "no workload — reachable only through SagaEventPublishers.fromEnvironment, which "
                            + "nothing in src/main/java calls. Cite it as a live permission need only "
                            + "once something wires it"));

    /**
     * The text shapes that build an SQS client, or wrap one in a publisher. A workload earns an SQS
     * permission by <em>constructing</em> one of these, not by containing a send: the send lives in a
     * shared publisher class that every workload reuses.
     *
     * <p>The two {@code fromEnvironment(} entries are qualified on purpose. They match a <em>caller</em>
     * of the factory, not the factory's own declaration, and a caller is precisely what a new workload
     * is.
     */
    private static final Set<String> CONSTRUCTION_MARKERS = Set.of(
            "SqsClient.builder(",
            "SqsClient.create(",
            "SqsAsyncClient.builder(",
            "SqsAsyncClient.create(",
            "new SqsAsyncEventPublisher(",
            "new SqsSagaEventPublisher(",
            "new SqsOutboxDeliveryAdapter(",
            "AsyncEventPublisherFactory.fromEnvironment(",
            "SagaEventPublishers.fromEnvironment(");

    /**
     * Every file that constructs an SQS client or an SQS-backed publisher, and the workload whose IAM
     * role therefore needs SQS permissions. This is the workload list the manifest comments are written
     * from; a new entry here is a new role to provision.
     */
    private static final Map<String, ConstructionSite> CONSTRUCTION_SITES = Map.of(
            "com/recsys/api/online/OnlinePredictionServer.java", new ConstructionSite(
                    Set.of("AsyncEventPublisherFactory.fromEnvironment("),
                    "online serving (k8s/base/online-serving.yaml) — createAsyncEventPublisher() asks "
                            + "the factory for the ONLINE_EVENTS prefix, which builds an SqsClient when "
                            + "ONLINE_EVENTS_SQS_ENABLED is set with a non-blank "
                            + "ONLINE_EVENTS_SQS_QUEUE_URL. This entry names no SQS type of its own, "
                            + "which is exactly why the send-site check missed this workload twice"),
            "com/recsys/config/ModelEventConfig.java", new ConstructionSite(
                    Set.of("SqsClient.builder(", "new SqsAsyncEventPublisher("),
                    "model serving (k8s/eks-shared/patches/irsa-model-serving.yaml) — the "
                            + "abExposurePublisher bean builds the client under recsys.events.sqs.enabled "
                            + "+ a non-blank recsys.events.sqs.queue-url; the recsys-model-serving "
                            + "ServiceAccount is currently bound to no pod"),
            "com/recsys/application/outbox/OutboxRelayCommand.java", new ConstructionSite(
                    Set.of("SqsAsyncClient.create(", "new SqsOutboxDeliveryAdapter("),
                    "outbox relay (k8s/base/outbox-relay-deployment.yaml) — builds the client under "
                            + "SAGA_EVENTS_SQS_ENABLED + a non-blank SAGA_EVENTS_SQS_QUEUE_URL, both off "
                            + "in k8s/base/configmap.yaml; the Deployment has no serviceAccountName, so "
                            + "there is no role to grant on yet"),
            "com/recsys/application/saga/SagaEventPublishers.java", new ConstructionSite(
                    Set.of("SqsClient.builder(", "new SqsSagaEventPublisher("),
                    "no workload — the client it builds and the SqsSagaEventPublisher it wraps it in "
                            + "are reachable only through SagaEventPublishers.fromEnvironment(), which "
                            + "nothing in src/main/java calls. No manifest comment cites it"),
            "com/recsys/infrastructure/messaging/AsyncEventPublisherFactory.java", new ConstructionSite(
                    Set.of("SqsClient.builder(", "new SqsAsyncEventPublisher("),
                    "shared factory, not a workload — the client it builds belongs to whichever "
                            + "workload calls fromEnvironment(prefix), and today that is only "
                            + "OnlinePredictionServer. A second caller is a second role: it gets its own "
                            + "entry above, and its own manifest comment"));

    /** The SQS send actions a single source file calls, and who needs the matching permission. */
    private record CallSite(Set<String> actions, String workloadNote) {}

    /** The SQS construction shapes a single source file uses, and whose role that obliges. */
    private record ConstructionSite(Set<String> markers, String workloadNote) {}

    @Test
    void sqsIsTheOnlyAwsServiceClientInMainSource() throws IOException {
        Map<String, Set<String>> byService = new TreeMap<>();
        for (Path file : javaSources()) {
            Matcher matcher = AWS_SERVICE_PACKAGE.matcher(Files.readString(file));
            while (matcher.find()) {
                byService.computeIfAbsent(matcher.group(1), s -> new TreeSet<>())
                        .add(relativePath(file));
            }
        }

        Map<String, Set<String>> undocumented = new TreeMap<>(byService);
        undocumented.keySet().removeAll(DOCUMENTED_SERVICES);

        assertThat(undocumented)
                .describedAs("These AWS SDK service clients are used in %s, but every IRSA comment in "
                        + "k8s/ is written on the premise that SQS is the only one — the gateway's "
                        + "role, in particular, is documented as needing no permissions BECAUSE the "
                        + "gateway calls no AWS API. A new service client means some workload now "
                        + "needs IAM actions nobody has written down. Update the manifest comment for "
                        + "the workload that uses it, then add the service here.", SOURCE_ROOT)
                .isEmpty();
    }

    @Test
    void everySqsSendCallSiteIsAttributedToAWorkload() throws IOException {
        Map<String, Set<String>> found = new TreeMap<>();
        for (Path file : javaSources()) {
            Set<String> actions = sendActionsIn(Files.readString(file));
            if (!actions.isEmpty()) {
                found.put(relativePath(file), actions);
            }
        }

        Set<String> unattributed = new TreeSet<>(found.keySet());
        unattributed.removeAll(SEND_CALL_SITES.keySet());
        assertThat(unattributed)
                .describedAs("These files send to SQS but no SEND_CALL_SITES entry says which "
                        + "workload's IAM role needs the permission. A workload that sends to SQS "
                        + "with no role granting it fails with AccessDenied the first time its flag "
                        + "is enabled — which is how online serving stayed undocumented through two "
                        + "reviews. Add the entry AND the manifest comment for its workload.")
                .isEmpty();

        Set<String> stale = new TreeSet<>(SEND_CALL_SITES.keySet());
        stale.removeAll(found.keySet());
        assertThat(stale)
                .describedAs("SEND_CALL_SITES claims these files send to SQS, but the scan no longer "
                        + "finds a send in them. Re-derive from source: the send moved or was removed "
                        + "(so a manifest comment now over-states what its role needs), or the file "
                        + "was renamed or moved and this map's key must follow.")
                .isEmpty();

        List<String> actionMismatches = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : found.entrySet()) {
            CallSite expected = SEND_CALL_SITES.get(entry.getKey());
            if (expected != null && !expected.actions().equals(entry.getValue())) {
                actionMismatches.add(entry.getKey() + ": calls " + entry.getValue()
                        + ", SEND_CALL_SITES expects " + expected.actions()
                        + " (needed by: " + expected.workloadNote() + ")");
            }
        }
        assertThat(actionMismatches)
                .describedAs("sqs:SendMessage and sqs:SendMessageBatch are distinct IAM actions. A "
                        + "publisher that switches between them silently invalidates the action named "
                        + "in its workload's manifest comment, and a role granting the other one "
                        + "fails closed with AccessDenied. Fix the comment and this map together.")
                .isEmpty();
    }

    @Test
    void everySqsClientConstructionSiteIsAttributedToAWorkload() throws IOException {
        Map<String, Set<String>> found = new TreeMap<>();
        for (Path file : javaSources()) {
            Set<String> markers = constructionMarkersIn(Files.readString(file));
            if (!markers.isEmpty()) {
                found.put(relativePath(file), markers);
            }
        }

        Set<String> unattributed = new TreeSet<>(found.keySet());
        unattributed.removeAll(CONSTRUCTION_SITES.keySet());
        assertThat(unattributed)
                .describedAs("These files build an SQS client or an SQS-backed publisher, but no "
                        + "CONSTRUCTION_SITES entry says which workload's IAM role that obliges. This "
                        + "is the check the send-site scan cannot do for you: a workload wired the way "
                        + "online serving is — AsyncEventPublisherFactory.fromEnvironment(prefix), "
                        + "reusing a publisher class that already exists — adds no send call site at "
                        + "all, and went undocumented through two reviews for exactly that reason. Add "
                        + "the entry AND the IRSA comment on the workload's manifest.")
                .isEmpty();

        Set<String> stale = new TreeSet<>(CONSTRUCTION_SITES.keySet());
        stale.removeAll(found.keySet());
        assertThat(stale)
                .describedAs("CONSTRUCTION_SITES claims these files build an SQS client, but the scan "
                        + "no longer finds one. Re-derive from source: the workload stopped using SQS "
                        + "(so its manifest comment now asks for permissions it does not need), or the "
                        + "file was renamed or moved and this map's key must follow.")
                .isEmpty();

        List<String> markerMismatches = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : found.entrySet()) {
            ConstructionSite expected = CONSTRUCTION_SITES.get(entry.getKey());
            if (expected != null && !expected.markers().equals(entry.getValue())) {
                markerMismatches.add(entry.getKey() + ": builds via " + entry.getValue()
                        + ", CONSTRUCTION_SITES expects " + expected.markers()
                        + " (needed by: " + expected.workloadNote() + ")");
            }
        }
        assertThat(markerMismatches)
                .describedAs("How a workload reaches SQS changed. That is not cosmetic: a file that "
                        + "swaps AsyncEventPublisherFactory for a directly built client, or picks up a "
                        + "second publisher, may now need a different action than its manifest comment "
                        + "names. Re-read the file, fix the comment, then update this map.")
                .isEmpty();
    }

    /**
     * The SQS send actions a source file calls. The trailing {@code (} keeps {@code sendMessageBatch}
     * from also counting as {@code sendMessage}, so the two actions stay distinguishable.
     */
    private static Set<String> sendActionsIn(String source) {
        Map<String, String> markers = new LinkedHashMap<>();
        markers.put(SEND_MESSAGE + "(", SEND_MESSAGE);
        markers.put(SEND_MESSAGE_BATCH + "(", SEND_MESSAGE_BATCH);

        Set<String> actions = new TreeSet<>();
        markers.forEach((marker, action) -> {
            if (source.contains(marker)) {
                actions.add(action);
            }
        });
        return actions;
    }

    /** The SQS client/publisher construction shapes a source file uses. */
    private static Set<String> constructionMarkersIn(String source) {
        Set<String> markers = new TreeSet<>();
        for (String marker : CONSTRUCTION_MARKERS) {
            if (source.contains(marker)) {
                markers.add(marker);
            }
        }
        return markers;
    }

    /** Package-qualified so two same-named classes in different packages cannot collide. */
    private static String relativePath(Path file) {
        return SOURCE_ROOT.relativize(file).toString().replace('\\', '/');
    }

    private static List<Path> javaSources() throws IOException {
        try (Stream<Path> files = Files.walk(SOURCE_ROOT)) {
            return files.filter(p -> p.toString().endsWith(".java")).sorted().toList();
        }
    }
}
