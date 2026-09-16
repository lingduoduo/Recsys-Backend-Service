package com.demo.retrieval.service.clients;

import com.demo.retrieval.service.clients.UserProfileValidation.Invalid;
import com.demo.retrieval.service.clients.UserProfileValidation.Result;
import com.demo.retrieval.service.clients.UserProfileValidation.Valid;
import com.demo.retrieval.support.ContractFixtures;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class UserProfileValidationTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROFILE_JSON = fixture()
        .replace("fixture-user", "u1")
        .replace("fixture-run", "run-7");

    @Test
    void nullRawProfileIsMissingProfile() {
        assertReason("missing_profile", UserProfileValidation.validate(null, "u1", "run-7", MAPPER));
    }

    @Test
    void unparseableJsonIsInvalidJson() {
        assertReason("invalid_json", UserProfileValidation.validate("not-json", "u1", "run-7", MAPPER));
    }

    @Test
    void wrongVersionIsUnsupportedVersion() {
        String json = PROFILE_JSON.replace("\"profile_version\":1", "\"profile_version\":2");
        assertReason("unsupported_version", UserProfileValidation.validate(json, "u1", "run-7", MAPPER));
    }

    @Test
    void differentUserIdIsUserMismatch() {
        String json = PROFILE_JSON.replace("\"u1\"", "\"u2\"");
        assertReason("user_mismatch", UserProfileValidation.validate(json, "u1", "run-7", MAPPER));
    }

    @Test
    void differentRunIsRunMismatch() {
        String json = PROFILE_JSON.replace("\"run-7\"", "\"run-8\"");
        assertReason("run_mismatch", UserProfileValidation.validate(json, "u1", "run-7", MAPPER));
    }

    @Test
    void validProfileIsReturnedWithNormalizedPreferenceNames() {
        String json = PROFILE_JSON.replace("\"sci-fi\"", "\"  SCI-FI  \"").replace("\"space\"", "\"Outer   Space\"");
        Result result = UserProfileValidation.validate(json, "u1", "run-7", MAPPER);
        Valid valid = assertInstanceOf(Valid.class, result);
        assertEquals("u1", valid.profile().userId());
        assertEquals("sci-fi", valid.profile().preferences().genres().get(0).value());
        assertEquals("outer space", valid.profile().preferences().tags().get(0).value());
        assertEquals(null, valid.profile().behavioralFeatures().averageRating());
    }

    private static void assertReason(String expected, Result result) {
        Invalid invalid = assertInstanceOf(Invalid.class, result);
        assertEquals(expected, invalid.reason());
    }

    private static String fixture() {
        return ContractFixtures.text("user_profile_v1.json");
    }
}
