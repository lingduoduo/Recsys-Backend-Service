package com.recsys.retrieval.service.clients;

import java.util.List;

public interface ImpressionBloomFilterClient {
    List<Long> getBloomFilterBits(String userId);
}
