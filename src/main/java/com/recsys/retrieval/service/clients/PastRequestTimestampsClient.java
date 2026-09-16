package com.demo.retrieval.service.clients;

import java.util.List;

public interface PastRequestTimestampsClient {
    List<Long> getTimestamps(String userId);
}
