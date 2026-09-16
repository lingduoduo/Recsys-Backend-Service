package com.recsys.retrieval.service.clients;

import java.util.List;

public interface PastRequestTimestampsClient {
    List<Long> getTimestamps(String userId);
}
