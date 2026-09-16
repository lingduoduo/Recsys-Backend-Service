package com.recsys.retrieval.support;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/** Frozen producer-contract snapshots used by standalone service tests. */
public final class ContractFixtures {

    private ContractFixtures() {
    }

    public static byte[] bytes(String name) {
        try (var input = ContractFixtures.class.getResourceAsStream("/contracts/" + name)) {
            if (input == null) {
                throw new IllegalStateException("Missing contract fixture: " + name);
            }
            return input.readAllBytes();
        } catch (IOException error) {
            throw new UncheckedIOException(error);
        }
    }

    public static String text(String name) {
        return new String(bytes(name), StandardCharsets.UTF_8);
    }
}
