package com.recsys.retrieval.config;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 7 moved {@code RecommendationProperties} from the bare {@code recsys} prefix to
 * {@code recsys.retrieval}, but its verification grepped for the literal string {@code "recsys.}
 * — which does not match a {@code @Value} binding, spelled {@code ${recsys....}}. Three bindings
 * were missed and kept reading keys nothing defines any more, silently taking their hard-coded
 * defaults: {@code RetrievalRecommendationController}'s item-embedding prefix and the
 * user-profile key-prefix in two Redis clients. See the Task 9 fix-round-1 report for the
 * concrete failure mode this would have produced once the ported {@code application.yml} landed.
 *
 * <p>This scans every {@code .java} file under the two retrieval source roots
 * ({@code com.recsys.retrieval} and {@code com.recsys.api.rest.retrieval}) for a
 * {@code ${recsys....}} placeholder and fails on any that is not spelled
 * {@code ${recsys.retrieval....}}. It would have caught all three misses above.
 */
class RetrievalValuePrefixTest {

    private static final List<Path> RETRIEVAL_SOURCE_ROOTS = List.of(
            Path.of("src/main/java/com/recsys/retrieval"),
            Path.of("src/main/java/com/recsys/api/rest/retrieval"));

    // Matches ${recsys.<something>, capturing what immediately follows "recsys."
    private static final Pattern RECSYS_PLACEHOLDER = Pattern.compile("\\$\\{recsys\\.([\\w.-]*)");

    @Test
    void noValueBindingUnderRetrievalUsesTheBareRecsysPrefix() throws IOException {
        List<String> offenders = new ArrayList<>();

        for (Path root : RETRIEVAL_SOURCE_ROOTS) {
            assertTrue(Files.isDirectory(root), root + " does not exist -- the retrieval source "
                    + "layout has moved and this test's roots need updating, or the scan is "
                    + "silently checking nothing");
            try (Stream<Path> files = Files.walk(root)) {
                for (Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                    String content = Files.readString(file);
                    Matcher matcher = RECSYS_PLACEHOLDER.matcher(content);
                    while (matcher.find()) {
                        if (!matcher.group(1).startsWith("retrieval.")) {
                            offenders.add(file + " -> ${recsys." + matcher.group(1));
                        }
                    }
                }
            }
        }

        assertTrue(offenders.isEmpty(),
                "Found @Value (or other property placeholder) bindings under the retrieval "
                        + "packages still spelled ${recsys.*} instead of ${recsys.retrieval.*}: "
                        + offenders + ". RecommendationProperties binds recsys.retrieval; a "
                        + "placeholder left under the bare recsys prefix reads a key nothing "
                        + "defines and silently falls back to its hard-coded default with no "
                        + "compile error and no failing test.");
    }
}
