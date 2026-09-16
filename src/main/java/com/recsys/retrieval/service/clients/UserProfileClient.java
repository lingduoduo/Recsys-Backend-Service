package com.recsys.retrieval.service.clients;

import com.recsys.retrieval.model.UserBehaviorProfile;

import java.util.Optional;

public interface UserProfileClient {
    Optional<UserBehaviorProfile> getProfile(String userId);
}
