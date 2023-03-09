package com.github.tgda.engine.core.payload;

import com.github.tgda.engine.core.term.RelationshipEntity;

import java.util.List;

public interface RelationshipEntitiesRetrieveResult {
    public List<RelationshipEntity> getRelationEntities();
    public EntitiesRetrieveStatistics getOperationStatistics();
}
