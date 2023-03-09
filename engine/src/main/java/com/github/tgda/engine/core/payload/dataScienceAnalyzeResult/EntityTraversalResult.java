package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

import com.github.tgda.engine.core.term.Entity;

import java.util.List;

public class EntityTraversalResult {

    private String startEntityUID;
    private List<String> entityTraversalFootprints;
    private List<Entity> traveledEntities;

    public EntityTraversalResult(String startEntityUID,List<String> entityTraversalFootprints,
                                 List<Entity> traveledEntities){
        this.startEntityUID = startEntityUID;
        this.entityTraversalFootprints = entityTraversalFootprints;
        this.traveledEntities = traveledEntities;
    }

    public String getStartEntityUID() {
        return startEntityUID;
    }

    public List<String> getEntityTraversalFootprints() {
        return entityTraversalFootprints;
    }

    public List<Entity> getTraveledEntities() {
        return traveledEntities;
    }
}
