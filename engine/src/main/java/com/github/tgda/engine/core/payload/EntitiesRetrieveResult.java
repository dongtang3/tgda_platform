package com.github.tgda.engine.core.payload;

import com.github.tgda.engine.core.term.Entity;

import java.util.List;

public interface EntitiesRetrieveResult {
    public List<Entity> getConceptionEntities();
    public EntitiesRetrieveStatistics getOperationStatistics();
}
