package com.github.tgda.engine.core.structure;

import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.RelationshipEntity;

import java.util.LinkedList;

public class EntitiesPath {

    private float pathWeight = Float.NaN;
    private String startEntityType;
    private String startEntityUID;
    private String endEntityType;
    private String endEntityUID;
    private int pathJumps;
    private LinkedList<Entity> pathConceptionEntities;
    private LinkedList<RelationshipEntity> pathRelationEntities;

    public EntitiesPath(String startEntityType, String startEntityUID, String endEntityType, String endEntityUID,
                        int pathJumps, LinkedList<Entity> pathConceptionEntities, LinkedList<RelationshipEntity> pathRelationEntities){
        this.startEntityType = startEntityType;
        this.startEntityUID = startEntityUID;
        this.endEntityType = endEntityType;
        this.endEntityUID = endEntityUID;
        this.pathJumps = pathJumps;
        this.pathConceptionEntities = pathConceptionEntities;
        this.pathRelationEntities = pathRelationEntities;
    }

    public EntitiesPath(String startEntityType, String startEntityUID, String endEntityType, String endEntityUID,
                        int pathJumps, LinkedList<Entity> pathConceptionEntities, LinkedList<RelationshipEntity> pathRelationEntities, float pathWeight){
        this.startEntityType = startEntityType;
        this.startEntityUID = startEntityUID;
        this.endEntityType = endEntityType;
        this.endEntityUID = endEntityUID;
        this.pathJumps = pathJumps;
        this.pathConceptionEntities = pathConceptionEntities;
        this.pathRelationEntities = pathRelationEntities;
        this.pathWeight = pathWeight;
    }

    public String getStartEntityType() {
        return startEntityType;
    }

    public String getStartEntityUID() {
        return startEntityUID;
    }

    public String getEndEntityType() {
        return endEntityType;
    }

    public String getEndEntityUID() {
        return endEntityUID;
    }

    public int getPathJumps() {
        return pathJumps;
    }

    public LinkedList<Entity> getPathConceptionEntities() {
        return pathConceptionEntities;
    }

    public LinkedList<RelationshipEntity> getPathRelationEntities() {
        return pathRelationEntities;
    }

    public float getPathWeight() {
        return this.pathWeight;
    }
}
