package com.demo.retrieval.service.clients;

import com.demo.retrieval.model.UserBehaviorProfile;

import java.util.Optional;

public interface UserProfileClient {
    Optional<UserBehaviorProfile> getProfile(String userId);
}
