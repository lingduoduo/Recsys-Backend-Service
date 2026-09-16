package com.demo.retrieval.model;

public record UserDemographics(
    int age,
    String gender,
    String occupation,
    String zipCode
) {
    public static UserDemographics empty() {
        return new UserDemographics(0, "", "", "");
    }
}
