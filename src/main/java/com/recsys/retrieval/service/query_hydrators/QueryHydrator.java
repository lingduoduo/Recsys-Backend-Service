package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.*;

public interface QueryHydrator<T> {
    T hydrate(T query);

    T update(T query, T hydrated);

    default boolean enable(T query) {
        return true;
    }

    default String name() {
        return getClass().getName();
    }
}
